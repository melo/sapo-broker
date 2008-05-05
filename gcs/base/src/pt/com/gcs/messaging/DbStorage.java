package pt.com.gcs.messaging;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.mina.common.IoSession;
import org.caudexorigo.ErrorAnalyser;
import org.caudexorigo.Shutdown;
import org.caudexorigo.cryto.MD5;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.conf.AgentInfo;

class DbStorage
{
	private static Logger log = LoggerFactory.getLogger(DbStorage.class);

	private static final String insert_sql = "INSERT INTO Message (msg_id, correlation_id, destination, priority, mtimestamp, expiration, source_app, content, sequence_nr, delivery_count, local_only) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String update_state_sql = "UPDATE Message SET delivery_count=delivery_count+1 WHERE msg_id = ? AND destination = ?";

	private static final String ack_sql = "DELETE FROM Message WHERE msg_id = ? AND destination = ?";

	private static final String fetch_all_msg_sql_ordered = "SELECT msg_id, correlation_id, destination, priority, mtimestamp, expiration, source_app, content, delivery_count, local_only FROM Message WHERE destination=? ORDER BY priority DESC, sequence_nr ASC";

	private static final String fetch_all_msg_sql_non_ordered = "SELECT msg_id, correlation_id, destination, priority, mtimestamp, expiration, source_app, content, delivery_count, local_only FROM Message WHERE destination=? LIMIT 19999";

	private static final String fetch_top_msg_sql = "SELECT msg_id, correlation_id, destination, priority, mtimestamp, expiration, source_app, content, delivery_count, local_only FROM Message WHERE  msg_id NOT IN (SELECT mid FROM TABLE(mid VARCHAR = ?)) AND destination=? LIMIT 1";

	private static final String count_msg_sql = "SELECT COUNT(*) FROM Message WHERE destination = ?";

	private static final String fetch_virtual_queues_sql = "SELECT queue_name FROM VirtualQueue;";

	private static final String insert_virtual_queue_sql = "MERGE INTO VirtualQueue KEY(queue_name) VALUES(?);";

	private static final String delete_virtual_queue_sql = "DELETE FROM VirtualQueue WHERE queue_name = ?";

	private static final String delete_queue_sql = "DELETE FROM Message WHERE destination = ?";

	private static final int MAX_DELIVERY_COUNT = 25;

	private static final long PRIORITY_ORDERING_THRESHOLD = 20000;

	private static final DbStorage instance = new DbStorage();

	protected static long count(String destinationName)
	{
		return instance.i_count(destinationName);
	}

	protected static boolean deleteMessage(String msgId, String queueName)
	{
		return instance.i_deleteMessage(msgId, queueName);
	}

	protected static void deleteQueue(String queueName)
	{
		instance.i_deleteQueue(queueName);
	}

	protected static void deleteVirtualQueue(String queueName)
	{
		instance.i_deleteVirtualQueue(queueName);
	}

	protected static String[] getVirtualQueuesNames()
	{
		return instance.i_getVirtualQueuesNames();
	}

	protected static void incrementDeliveryCount(String msgid, String destinationName)
	{
		instance.i_incrementDeliveryCount(msgid, destinationName);
	}

	protected static void insert(Message msg, long sequence, int deliveryCount, boolean localConsumersOnly)
	{
		instance.i_insert(msg, sequence, deliveryCount, localConsumersOnly);
	}

	protected static Message poll(final QueueProcessor processor)
	{
		return instance.i_poll(processor);
	}

	protected static void recoverMessages(final QueueProcessor processor)
	{
		instance.i_recoverMessages(processor);
	}

	protected static void saveVirtualQueue(String queue_name)
	{
		instance.i_saveVirtualQueue(queue_name);
	}

	private Connection conn;

	private String driverName;

	private String connURL;

	private String username;

	private String password;

	private String dbFile;

	private String dbName;

	private PreparedStatement insert_prep_stmt;

	private PreparedStatement update_state_prep_stmt;

	private PreparedStatement ack_state_prep_stmt;

	private PreparedStatement count_msg_prep_stmt;

	private PreparedStatement insert_virtual_queue_prep_stmt;

	private PreparedStatement delete_virtual_queue_prep_stmt;

	private PreparedStatement delete_queue_prep_stmt;

	private PreparedStatement fetch_top_msg_prep_stmt;

	private DbStorage()
	{
		try
		{
			driverName = "org.h2.Driver";
			dbFile = AgentInfo.getBasePersistentDirectory().concat("/");
			dbName = MD5.getHashString(AgentInfo.getAgentName());

			connURL = "jdbc:h2:file:" + dbFile.concat(dbName).concat(";LOG=1;MAX_MEMORY_UNDO=10000;MAX_MEMORY_ROWS=20000;WRITE_DELAY=200;CACHE_TYPE=TQ;RECOVER=1");
			username = "sa";
			password = "";

			Class.forName(driverName);

			conn = DriverManager.getConnection(connURL, username, password);

			buildSchema();
			buildStatments();

			log.info("Persistent storage is ready.");
		}
		catch (Throwable t)
		{
			dealWithError(t, false);
			closeQuietly(conn);
			Shutdown.now();
		}
	}

	private void batchUpdateState(QueueProcessor qproc)
	{
		try
		{
			synchronized (update_state_prep_stmt)
			{
				for (String mid : qproc.getAckWaitList())
				{
					update_state_prep_stmt.setString(1, mid);
					update_state_prep_stmt.setString(2, qproc.getDestinationName());
					update_state_prep_stmt.executeUpdate();
				}
			}
		}
		catch (Throwable t)
		{
			dealWithError(t, false);
		}
	}

	private Message buildMessage(ResultSet rs) throws SQLException
	{
		final Message msg = new Message(rs.getString(1), rs.getString(3), rs.getString(8));
		msg.setCorrelationId(rs.getString(2));
		msg.setPriority(rs.getInt(4));
		msg.setTimestamp(rs.getLong(5));
		msg.setExpiration(rs.getLong(6));
		msg.setSourceApp(rs.getString(7));
		return msg;
	}

	private synchronized void buildSchema() throws Throwable
	{
		BufferedReader in = new BufferedReader(new InputStreamReader(DbStorage.class.getResourceAsStream("/pt/com/gcs/etc/create_schema.sql")));
		String sql;
		while ((sql = in.readLine()) != null)
		{
			runActionSql(conn, sql);
		}
		in.close();
	}

	private synchronized void buildStatments()
	{
		try
		{
			conn = DriverManager.getConnection(connURL, username, password);
			insert_prep_stmt = conn.prepareStatement(insert_sql);
			update_state_prep_stmt = conn.prepareStatement(update_state_sql);
			ack_state_prep_stmt = conn.prepareStatement(ack_sql);
			count_msg_prep_stmt = conn.prepareStatement(count_msg_sql);
			insert_virtual_queue_prep_stmt = conn.prepareStatement(insert_virtual_queue_sql);
			delete_virtual_queue_prep_stmt = conn.prepareStatement(delete_virtual_queue_sql);
			delete_queue_prep_stmt = conn.prepareStatement(delete_queue_sql);
			fetch_top_msg_prep_stmt = conn.prepareStatement(fetch_top_msg_sql);
		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			rt.printStackTrace();
			closeQuietly(conn);
			Shutdown.now();
		}
	}

	private void closeQuietly(Connection connection)
	{
		try
		{
			if (connection != null)
			{
				connection.close();
			}
		}
		catch (SQLException e)
		{
			// quiet
		}
	}

	private void closeQuietly(ResultSet rs)
	{
		try
		{
			if (rs != null)
			{
				rs.close();
			}
		}
		catch (SQLException e)
		{
			// quiet
		}
	}

	private void closeQuietly(Statement stmt)
	{
		try
		{
			if (stmt != null)
			{
				stmt.close();
			}
		}
		catch (SQLException e)
		{
			// quiet
		}
	}

	private void dealWithError(Throwable t, boolean rethrow)
	{
		Throwable rt = ErrorAnalyser.findRootCause(t);
		log.error(rt.getMessage(), rt);
		ErrorAnalyser.exitIfOOM(rt);
		if (rethrow)
		{
			throw new RuntimeException(rt);
		}
	}

	private long i_count(String destinationName)
	{
		synchronized (count_msg_prep_stmt)
		{
			ResultSet rs = null;
			try
			{
				count_msg_prep_stmt.setString(1, destinationName);
				rs = count_msg_prep_stmt.executeQuery();
				rs.next();
				return rs.getLong(1);
			}
			catch (Throwable t)
			{
				dealWithError(t, false);
				return 0;
			}
			finally
			{
				closeQuietly(rs);
			}
		}
	}

	private boolean i_deleteMessage(String msgId, String queueName)
	{
		synchronized (ack_state_prep_stmt)
		{
			try
			{
				ack_state_prep_stmt.setString(1, msgId);
				ack_state_prep_stmt.setString(2, queueName);
				return (ack_state_prep_stmt.executeUpdate() > 0);
			}
			catch (Throwable t)
			{
				dealWithError(t, false);
				return false;
			}
		}
	}

	private void i_deleteQueue(String queueName)
	{
		synchronized (delete_queue_prep_stmt)
		{
			try
			{
				delete_queue_prep_stmt.setString(1, queueName);
				delete_queue_prep_stmt.executeUpdate();
			}
			catch (Throwable t)
			{
				dealWithError(t, false);
			}
		}
	}

	private void i_deleteVirtualQueue(String queueName)
	{
		synchronized (delete_virtual_queue_prep_stmt)
		{
			try
			{
				delete_virtual_queue_prep_stmt.setString(1, queueName);
				delete_virtual_queue_prep_stmt.executeUpdate();
			}
			catch (Throwable t)
			{
				dealWithError(t, false);
			}
		}
	}

	private String[] i_getVirtualQueuesNames()
	{
		ResultSet rs = null;
		List<String> lst_q = new ArrayList<String>();

		try
		{
			Statement fetch_stmt = conn.createStatement();
			rs = fetch_stmt.executeQuery(fetch_virtual_queues_sql);

			while (rs.next())
			{
				lst_q.add(rs.getString(1));
			}
			return lst_q.toArray(new String[lst_q.size()]);
		}
		catch (Throwable t)
		{
			dealWithError(t, true);
			return new String[0];
		}
		finally
		{
			closeQuietly(rs);
		}
	}

	protected void i_incrementDeliveryCount(String msgid, String destinationName)
	{
		try
		{
			synchronized (update_state_prep_stmt)
			{
				update_state_prep_stmt.setString(1, msgid);
				update_state_prep_stmt.setString(2, destinationName);
				update_state_prep_stmt.executeUpdate();
			}
		}
		catch (Throwable t)
		{
			dealWithError(t, false);
		}
	}

	private void i_insert(Message msg, long sequence, int deliveryCount, boolean localConsumersOnly)
	{
		synchronized (insert_prep_stmt)
		{
			try
			{

				insert_prep_stmt.setString(1, msg.getMessageId());
				insert_prep_stmt.setString(2, msg.getCorrelationId());
				insert_prep_stmt.setString(3, msg.getDestination());
				insert_prep_stmt.setInt(4, msg.getPriority());
				insert_prep_stmt.setLong(5, msg.getTimestamp());
				insert_prep_stmt.setLong(6, msg.getExpiration());
				insert_prep_stmt.setString(7, msg.getSourceApp());
				insert_prep_stmt.setString(8, msg.getContent());
				insert_prep_stmt.setLong(9, sequence);
				insert_prep_stmt.setInt(10, deliveryCount);
				insert_prep_stmt.setBoolean(11, localConsumersOnly);
				insert_prep_stmt.executeUpdate();
			}
			catch (Throwable t)
			{
				dealWithError(t, true);
			}
		}
	}

	private Message i_poll(final QueueProcessor processor)
	{
		synchronized (processor)
		{
			ResultSet rs = null;
			try
			{
				Set<String> reservedMessages = processor.getReservedMessages();
				Set<String> ackWaitList = processor.getAckWaitList();
				String[] eq_mids0 = reservedMessages.toArray(new String[reservedMessages.size()]);
				String[] eq_mids1 = ackWaitList.toArray(new String[ackWaitList.size()]);

				String[] eq_mids = new String[eq_mids0.length + eq_mids1.length];

				System.arraycopy(eq_mids0, 0, eq_mids, 0, eq_mids0.length);
				System.arraycopy(eq_mids1, 0, eq_mids, eq_mids0.length, eq_mids1.length);

				fetch_top_msg_prep_stmt.setObject(1, eq_mids);
				fetch_top_msg_prep_stmt.setString(2, processor.getDestinationName());
				rs = fetch_top_msg_prep_stmt.executeQuery();

				if (rs.next())
				{
					log.debug("Get head message for queue '{}'", processor.getDestinationName());
					Message msg = buildMessage(rs);

					int deliveryCount = rs.getInt(9);

					long mark = System.currentTimeMillis();

					if ((mark <= msg.getExpiration()) && (deliveryCount <= MAX_DELIVERY_COUNT))
					{
						reservedMessages.add(msg.getMessageId());
						return msg;
					}
					else
					{
						log.warn("Expired or overdelivered message: '{}' id: '{}'", msg.getDestination(), msg.getMessageId());
						deleteMessage(msg.getMessageId(), processor.getDestinationName());
						processor.decrementMsgCounter();
						return i_poll(processor);
					}
				}
				else
				{
					return null;
				}

			}
			catch (Throwable t)
			{
				dealWithError(t, false);
				return i_poll(processor);
			}
			finally
			{
				closeQuietly(rs);
			}
		}

	}

	private void i_recoverMessages(final QueueProcessor processor)
	{
		ResultSet rs = null;
		try
		{
			long count = processor.getQueuedMessagesCount();

			PreparedStatement fetch_stmt;
			if (count < PRIORITY_ORDERING_THRESHOLD)
			{
				fetch_stmt = conn.prepareStatement(fetch_all_msg_sql_ordered);
			}
			else
			{
				fetch_stmt = conn.prepareStatement(fetch_all_msg_sql_non_ordered);
			}

			fetch_stmt.setString(1, processor.getDestinationName());
			rs = fetch_stmt.executeQuery();
			log.debug("Processing stored messages for queue '{}'", processor.getDestinationName());
			while (rs.next() && processor.hasRecipient())
			{
				final Message msg = buildMessage(rs);
				int deliveryCount = rs.getInt(9);
				final boolean localConsumersOnly = rs.getBoolean(10);

				if (!processor.getReservedMessages().contains(msg.getMessageId()))
				{
					processMessage(processor, msg, deliveryCount, localConsumersOnly);
				}
			}
			batchUpdateState(processor);
		}
		catch (Throwable t)
		{
			dealWithError(t, false);
		}
		finally
		{
			closeQuietly(rs);
		}
	}

	private void i_saveVirtualQueue(String queue_name)
	{
		synchronized (insert_virtual_queue_prep_stmt)
		{
			try
			{
				insert_virtual_queue_prep_stmt.setString(1, queue_name);
				insert_virtual_queue_prep_stmt.executeUpdate();
			}
			catch (Throwable t)
			{
				dealWithError(t, false);
			}
		}
	}

	private void processMessage(final QueueProcessor processor, final Message msg, int deliveryCount, final boolean localConsumersOnly)
	{
		long mark = System.currentTimeMillis();

		if ((mark <= msg.getExpiration()) && (deliveryCount <= MAX_DELIVERY_COUNT))
		{
			if (processor.forward(msg, localConsumersOnly))
			{
				if (log.isDebugEnabled())
				{
					log.debug("Message delivered. Dump: {}", msg.toString());
				}
			}
			else
			{
				if (log.isDebugEnabled())
				{
					log.debug("Could not deliver message. Dump: {}", msg.toString());
				}
				log.warn("Could not deliver message. Dump: {}", msg.toString());
			}
		}
		else
		{
			log.warn("Expired or overdelivered message: '{}' id: '{}'", msg.getDestination(), msg.getMessageId());
			deleteMessage(msg.getMessageId(), processor.getDestinationName());
			processor.decrementMsgCounter();
		}
	}

	private boolean runActionSql(Connection connection, String sql)
	{
		boolean success = false;
		Statement statement = null;
		try
		{
			if (StringUtils.isNotBlank(sql))
			{
				statement = connection.createStatement();
				success = statement.execute(sql);
			}
		}
		catch (Throwable t)
		{
			dealWithError(t, false);
			closeQuietly(connection);
			Shutdown.now();
		}
		finally
		{
			closeQuietly(statement);
		}
		return success;
	}

}
