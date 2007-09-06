package pt.com.gcs.io;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Shutdown;
import pt.com.gcs.conf.AgentInfo;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.QueueProcessor;

public class DbStorage
{
	private static Logger log = LoggerFactory.getLogger(DbStorage.class);

	private static final String insert_sql = "INSERT INTO Message (destination, priority, timestamp, sequence_nr, msg_id, delivery_count, msg) VALUES (?, ?, ?, ?, ?, ?, ?)";

	private static final String update_state_sql = "UPDATE Message SET delivery_count=delivery_count+1 WHERE msg_id = ?;";

	private static final String ack_sql = "DELETE FROM Message WHERE msg_id = ?;";

	private static final String fetch_msg_sql = "SELECT msg FROM Message WHERE destination = ? AND delivery_count <= ? ORDER BY priority ASC, timestamp ASC, sequence_nr ASC";

	private static final String count_msg_sql = "SELECT COUNT(*) FROM Message WHERE destination = ? AND delivery_count=0";

	private Connection conn;

	private String driverName;

	private String connURL;

	private String username;

	private String password;

	private PreparedStatement insert_prep_stmt;

	private PreparedStatement update_state_prep_stmt;

	private PreparedStatement ack_state_prep_stmt;

	private PreparedStatement fetch_msg_prep_stmt;

	private PreparedStatement count_msg_prep_stmt;

	private Object ack_mutex = new Object();

	private Object insert_mutex = new Object();

	private Object pqueue_mutex = new Object();

	private Object count_mutex = new Object();
	
	private final ConcurrentMap<String, String> updateMsgBuffer = new ConcurrentHashMap<String, String>();

	public DbStorage()
	{
		try
		{
			driverName = "org.h2.Driver"; //org.h2.Driver
			connURL = "jdbc:h2:file:" + AgentInfo.getBasePersistentDirectory() .concat("/").concat(AgentInfo.getAgentName()).concat(";LOG=0;MAX_MEMORY_UNDO=1000;MAX_MEMORY_ROWS=1000");
			username = "sa";
			password = "";

			 System.out.println("driverName:" + driverName);
			 System.out.println("connURL:" + connURL);
			 System.out.println("username:" + username);
			 System.out.println("password:" + password);

			Class.forName(driverName);
			conn = DriverManager.getConnection(connURL, username, password);

			buildSchema();

			insert_prep_stmt = conn.prepareStatement(insert_sql);
			update_state_prep_stmt = conn.prepareStatement(update_state_sql);
			ack_state_prep_stmt = conn.prepareStatement(ack_sql);
			fetch_msg_prep_stmt = conn.prepareStatement(fetch_msg_sql);
			count_msg_prep_stmt = conn.prepareStatement(count_msg_sql);
		}
		catch (Throwable e)
		{
			e.printStackTrace();
			Shutdown.now();
		}
	}

	public void ackMessage(Message msg)
	{
		synchronized (ack_mutex)
		{
			try
			{
				ack_state_prep_stmt.setString(1, msg.getAckId());
				ack_state_prep_stmt.execute();
				updateMsgBuffer.remove(msg.getAckId());
			}
			catch (Throwable t)
			{
				buildConnection();
				throw new RuntimeException(t);
			}
		}
	}

	private synchronized void buildConnection()
	{
		closeQuietly(conn);

		try
		{
			conn = DriverManager.getConnection(connURL, username, password);
			insert_prep_stmt = conn.prepareStatement(insert_sql);
			update_state_prep_stmt = conn.prepareStatement(update_state_sql);
			ack_state_prep_stmt = conn.prepareStatement(ack_sql);
			fetch_msg_prep_stmt = conn.prepareStatement(fetch_msg_sql);
		}
		catch (Throwable t)
		{
			log.error(t.getMessage(), t);
			Shutdown.now();
		}
	}

	private void buildSchema() throws Throwable
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

	public long count(String destinationName)
	{
		synchronized (count_mutex)
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
				buildConnection();
				throw new RuntimeException(t);
			}
			finally
			{
				closeQuietly(rs);
			}
		}
	}

	public void deliverStoredMessages(QueueProcessor processor, int threshold)
	{
		System.out.println("DbStorage.deliverStoredMessages()");
		synchronized (pqueue_mutex)
		{
			ResultSet rs = null;
			try
			{
				String destinationName = processor.getDestinationName();
				fetch_msg_prep_stmt.setString(1, destinationName);
				fetch_msg_prep_stmt.setInt(2, threshold);
				rs = fetch_msg_prep_stmt.executeQuery();

				
				while (rs.next())
				{
					Message msg = Message.fromString(rs.getString(1));

					if (processor.deliverMessage(msg))
					{
						updateMsgBuffer.put(msg.getMessageId(), msg.getMessageId());
						log.debug("Message delivered. Dump: {}", msg.toString());
					}
					else
					{
						log.debug("Could not deliver message. Dump: {}", msg.toString());
						break;
					}
				}
				batchUpdateState();
			}
			catch (Throwable t)
			{
				buildConnection();
				throw new RuntimeException(t);
			}
			finally
			{
				closeQuietly(rs);
			}
		}
	}

	public void insert(Message msg, long sequence, int deliveryCount)
	{
		synchronized (insert_mutex)
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
				insert_prep_stmt.execute();
			}
			catch (Throwable t)
			{
				buildConnection();
				throw new RuntimeException(t);
			}
		}
	}

	private int runActionSql(Connection connection, String sql)
	{
		int numRecordsUpdated = 0;
		Statement statement = null;
		try
		{
			statement = connection.createStatement();
			numRecordsUpdated = statement.executeUpdate(sql);
		}
		catch (Throwable t)
		{
			buildConnection();
			throw new RuntimeException(t);
		}
		finally
		{
			closeQuietly(statement);
		}
		return numRecordsUpdated;
	}


	private void batchUpdateState()
	{
		try
		{
			conn.setAutoCommit(false);

			Set<String> ids = updateMsgBuffer.keySet();
			List<String> idlist = new ArrayList<String>();
			for (String id : ids)
			{
				idlist.add(id);
			}
			
			for (String id : idlist)
			{
				update_state_prep_stmt.setString(1, id);
				update_state_prep_stmt.addBatch();
				updateMsgBuffer.remove(id);
			}
			idlist.clear();
			update_state_prep_stmt.executeBatch();
			conn.commit();
		}
		catch (Throwable t)
		{
			buildConnection();
			throw new RuntimeException(t);
		}
	}
}
