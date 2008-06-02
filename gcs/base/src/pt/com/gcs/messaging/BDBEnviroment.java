package pt.com.gcs.messaging;

import java.io.File;

import org.caudexorigo.ErrorAnalyser;
import org.caudexorigo.Shutdown;
import org.caudexorigo.cryto.MD5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.conf.GcsInfo;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BDBEnviroment
{
	private static Logger log = LoggerFactory.getLogger(BDBEnviroment.class);

	private Environment env;

	private String dbFile;

	private String dbName;

	private String dbDir;

	private static final BDBEnviroment instance = new BDBEnviroment();

	private BDBEnviroment()
	{
		try
		{
			dbFile = GcsInfo.getBasePersistentDirectory().concat("/");
			dbName = MD5.getHashString(GcsInfo.getAgentName());

			dbDir = dbFile.concat(dbName);
			(new File(dbDir)).mkdirs();

			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setTransactional(true);
			envConfig.setTxnWriteNoSync(true);
			envConfig.setTxnNoSync(true);
			env = new Environment(new File(dbDir), envConfig);

		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			log.error(rt.getMessage(), rt);
			Shutdown.now();
		}
	}

	public static Environment get()
	{
		return instance.env;
	}

	public static void sync()
	{
		try
		{
			instance.env.sync();
			log.info("Sync was successful");
		}
		catch (DatabaseException e)
		{
			log.error(e.getMessage(), e);
		}
	}
}
