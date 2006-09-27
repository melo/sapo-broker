/*
 * Copyright 2006 The asyncWeb Team.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pt.com.http;

import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.safehaus.asyncweb.container.ServiceContainer;
import org.safehaus.asyncweb.container.basic.BasicServiceContainer;
import org.safehaus.asyncweb.container.basic.HttpServiceHandler;
import org.safehaus.asyncweb.container.resolver.SimplePrefixResolver;
import org.safehaus.asyncweb.transport.nio.HttpIOHandler;
import org.safehaus.asyncweb.transport.nio.NIOTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncWebStandalone
{
	private static final Logger LOG = LoggerFactory.getLogger(AsyncWebStandalone.class);

	private int _portNumber;

	public AsyncWebStandalone(int portNumber)
	{
		_portNumber = portNumber;
	}

	public void start(SocketAcceptor acceptor)
	{
		try
		{
			NIOTransport transport = new NIOTransport(acceptor);
			transport.setPort(_portNumber);
			HttpIOHandler httpIOHandler = new HttpIOHandler();
			httpIOHandler.setMaxKeepAlives(100);
			httpIOHandler.setReadIdleTime(300);
			transport.setHttpIOHandler(httpIOHandler);

			HttpServiceHandler httpHandler = new HttpServiceHandler();
			SimplePrefixResolver presolver = new SimplePrefixResolver();
			presolver.setUriPrefix("/broker/");
			httpHandler.setServiceResolver(presolver);
			httpHandler.addHttpService("producer", new BrokerWsHttpService());
			httpHandler.addHttpService("mng", new ManagementService());
			ServiceContainer container = new BasicServiceContainer();
			container.addTransport(transport);
			container.addServiceHandler(httpHandler);

			container.start();
			LOG.info("AsyncWeb server started");
		}
		catch (Throwable e)
		{
			LOG.error("Failed to start HTTP container", e);
			System.exit(1);
		}

	}

}
