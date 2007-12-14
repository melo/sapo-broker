package pt.com.gcs.io;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.caudexorigo.lang.ErrorAnalyser;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Shutdown;
import pt.com.gcs.conf.AgentInfo;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.QueueProcessor;

public class DbStorage
{
	private static Logger log = LoggerFactory.getLogger(DbStorage.class);

	private static final DbStorage instance = new DbStorage();

	private static final String insert_sql = "INSERT INTO Message (destination, priority, mtimestamp, sequence_nr, msg_id, delivery_count, msg) VALUES (?, ?, ?, ?, ?, ?, ?)";

	private static final String update_state_sql = "UPDATE Message SET delivery_count=delivery_count+1 WHERE msg_id = ?";

	private static final String ack_sql = "DELETE FROM Message WHERE msg_id = ?";

	private static final String fetch_msg_sql = "SELECT msg FROM Message WHERE destination = ? ORDER BY priority ASC, mtimestamp ASC, sequence_nr ASC";

	private static final String count_msg_sql = "SELECT COUNT(*) FROM Message WHERE destination = ?";

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

	private final ConcurrentMap<String, String> msgsAwaitingAck = new ConcurrentHashMap<String, String>();

	private DbStorage()
	{
		try
		{
			driverName = "org.h2.Driver"; // org.h2.Driver
			dbFile = AgentInfo.getBasePersistentDirectory().concat("/");
			dbName = AgentInfo.getAgentName();
			connURL = "jdbc:h2:file:" + dbFile.concat(dbName).concat(";LOG=0;MAX_MEMORY_UNDO=1000;MAX_MEMORY_ROWS=1000;WRITE_DELAY=5000");
			username = "sa";
			password = "";

			Class.forName(driverName);

			conn = DriverManager.getConnection(connURL, username, password);

			buildSchema();
			buildStatments();
		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			log.error(rt.getMessage(), rt);
			closeQuietly(conn);
			Shutdown.now();
		}
	}

	public static void init()
	{
		instance.iinit();
	}

	public void iinit()
	{
		log.info("Persistent storage is ready.");
	}

	public static void ackMessage(String msgId)
	{
		instance.msgsAwaitingAck.remove(msgId);
		deleteMessage(msgId);
	}

	public static void deleteMessage(String msgId)
	{
		instance.ideleteMessage(msgId);
	}

	private void ideleteMessage(String msgId)
	{
		synchronized (ack_state_prep_stmt)
		{
			try
			{
				ack_state_prep_stmt.setString(1, msgId);
				ack_state_prep_stmt.executeUpdate();
			}
			catch (Throwable t)
			{
				Throwable rt = ErrorAnalyser.findRootCause(t);
				log.error(rt.getMessage(), rt);
			}
		}

	}

	private void batchUpdateState()
	{
		try
		{
			Set<String> ids = msgsAwaitingAck.keySet();

			synchronized (update_state_prep_stmt)
			{
				for (String id : ids)
				{
					String mid = msgsAwaitingAck.remove(id);
					update_state_prep_stmt.setString(1, mid);
					update_state_prep_stmt.executeUpdate();
				}
			}
		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			log.error(rt.getMessage(), rt);
		}
	}

	private synchronized void buildStatments()
	{
		// closeQuietly(conn);
		try
		{
			conn = DriverManager.getConnection(connURL, username, password);
			insert_prep_stmt = conn.prepareStatement(insert_sql);
			update_state_prep_stmt = conn.prepareStatement(update_state_sql);
			ack_state_prep_stmt = conn.prepareStatement(ack_sql);
			count_msg_prep_stmt = conn.prepareStatement(count_msg_sql);
		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			rt.printStackTrace();
			closeQuietly(conn);
			Shutdown.now();
		}
	}

	private synchronized void buildSchema() throws Throwable
	{
		BufferedReader in = new BufferedReader(new InputStreamReader(DbStorage.class.getResourceAsStream("/create_schema.sql")));
		String sql;
		while ((sql = in.readLine()) != null)
		{
			runActionSql(conn, sql);
		}
		in.close();
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

	public static long count(String destinationName)
	{
		return instance.icount(destinationName);
	}

	private long icount(String destinationName)
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
				Throwable rt = ErrorAnalyser.findRootCause(t);
				log.error(rt.getMessage(), rt);
				return 0;
			}
			finally
			{
				closeQuietly(rs);
			}
		}
	}

	public static void recoverMessages(QueueProcessor processor)
	{
		instance.irecoverMessages(processor);
	}

	public void irecoverMessages(QueueProcessor processor)
	{
		ResultSet rs = null;
		try
		{
			String destinationName = processor.getDestinationName();

			PreparedStatement fetch_stmt = conn.prepareStatement(fetch_msg_sql);
			fetch_stmt.setString(1, destinationName);
			rs = fetch_stmt.executeQuery();

			while (rs.next())
			{
				Message msg = Message.fromString(rs.getString(1));

				if (processor.deliverMessage(msg))
				{
					msgsAwaitingAck.put(msg.getMessageId(), msg.getMessageId());
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
					break;
				}
			}

			batchUpdateState();
		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			log.error(rt.getMessage(), rt);
		}
		finally
		{
			closeQuietly(rs);
		}

	}

	public static void insert(Message msg, long sequence, int deliveryCount)
	{
		instance.iinsert(msg, sequence, deliveryCount);
	}

	public void iinsert(Message msg, long sequence, int deliveryCount)
	{
		synchronized (insert_prep_stmt)
		{
			try
			{
				insert_prep_stmt.setString(1, msg.getDestination());
				insert_prep_stmt.setInt(2, msg.getPriority());
				insert_prep_stmt.setLong(3, System.currentTimeMillis());
				insert_prep_stmt.setLong(4, sequence);
				insert_prep_stmt.setString(5, msg.getMessageId());
				insert_prep_stmt.setInt(6, deliveryCount);
				insert_prep_stmt.setString(7, msg.toString());
				insert_prep_stmt.executeUpdate();
				if (deliveryCount > 0)
				{
					msgsAwaitingAck.put(msg.getMessageId(), msg.getMessageId());
				}
			}
			catch (Throwable t)
			{
				Throwable rt = ErrorAnalyser.findRootCause(t);
				log.error("Could not save message: {}", rt.getMessage());
			}
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
			closeQuietly(conn);
			try
			{
				conn = DriverManager.getConnection(connURL, username, password);
			}
			catch (SQLException e)
			{
				System.out.println("Error creating connection:\n" + e.getMessage());
			}
			Throwable rt = ErrorAnalyser.findRootCause(t);
			log.error(rt.getMessage(), rt);
			closeQuietly(conn);
			Shutdown.now();
		}
		finally
		{
			closeQuietly(statement);
		}
		return success;
	}
}
