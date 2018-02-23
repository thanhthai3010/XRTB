package com.xrtb.bidder;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xrtb.commands.Echo;
import com.xrtb.common.Configuration;
import com.xrtb.common.SSL;
import com.xrtb.fraud.ForensiqClient;
import com.xrtb.jmq.WebMQ;
import com.xrtb.pojo.BidRequest;
import com.xrtb.tools.DbTools;
import com.xrtb.tools.Performance;

/**
 * A JAVA based RTB2.2 server.<br>
 * This is the RTB Bidder's main class. It is a Jetty based http server that
 * encapsulates the Jetty server. The class is Runnable, with the Jetty server
 * joining in the run method. This allows other parts of the bidder to interact
 * with the server, mainly to obtain status information from a command sent via
 * REDIS.
 * <p>
 * Prior to calling the RTBServer the configuration file must be Configuration
 * instance needs to be created - which is a singleton. The RTBServer class (as
 * well as many of the other classes also use configuration data in this
 * singleton.
 * <p>
 * A Jetty based Handler class is used to actually process all of the HTTP
 * requests coming into the bidder system.
 * <p>
 * Once the RTBServer.run method is invoked, the Handler is attached to the
 * Jetty server.
 * 
 * @author Ben M. Faul
 * 
 */
public class RTBServer implements Runnable {

    /** Logging object */
    protected static final Logger logger = LoggerFactory.getLogger(RTBServer.class);

    /** The url of the simulator */
    public static final String SIMULATOR_URL = "/xrtb/simulator/exchange";
    /** The url of where the simulator's resources live */
    public static final String SIMULATOR_ROOT = "web/exchange.html";

    public static final String CAMPAIGN_URL = "/xrtb/simulator/campaign";
    public static final String LOGIN_URL = "/xrtb/simulator/login";
    public static final String ADMIN_URL = "/xrtb/simulator/admin";
    public static final String CAMPAIGN_ROOT = "web/test.html";
    public static final String LOGIN_ROOT = "web/login.html";
    public static final String ADMIN_ROOT = "web/admin.html";

    /** Period for updateing performance stats in redis */
    public static final int PERIODIC_UPDATE_TIME = 60000;
    /** Pertiod for updating performance stats in Zookeeper */
    public static final int ZOOKEEPER_UPDATE = 5000;

    /** The strategy to find bids */
    public static int strategy = Configuration.STRATEGY_MAX_CONNECTIONS;;
    /** The HTTP code for a bid object */
    public static final int BID_CODE = 200; // http code ok
    /** The HTTP code for a no-bid objeect */
    public static final int NOBID_CODE = 204; // http code no bid

    /** The percentage of bid requests to consider for bidding */
    public static AtomicLong percentage = new AtomicLong(100); // throttle is
							       // wide open at
							       // 100, closed
    // at 0

    /** Indicates of the server is not accepting bids */
    public static boolean stopped = false; // is the server not accepting bid
					   // requests?

    public static boolean paused = false; // used to temporarially pause, so
					  // queues can drain, for example

    /** number of threads in the jetty thread pool */
    public static int threads = 1024;

    /**
     * a counter for the number of requests the bidder has received and processed
     */
    public static long request = 0;
    /** Counter for number of bids made */
    public static long bid = 0; // number of bids processed
    /** Counter for number of nobids made */
    public static long nobid = 0; // number of nobids processed
    /** Number of errors in accessing the bidder */
    public static long error = 0;
    /** Number of actual requests */
    public static long handled = 0;
    /** Number of unknown accesses */
    public static long unknown = 0;
    /** The configuration of the bidder */
    public static Configuration config;
    /** The number of win notifications */
    public volatile static long win = 0;
    /** The number of clicks processed */
    public volatile static long clicks = 0;
    /** The number of pixels fired */
    public volatile static long pixels = 0;
    /** The average time */
    public volatile static long avgBidTime;
    /** Fraud counter */
    public volatile static long fraud = 0;
    /** xtime counter */
    public volatile static long xtime = 0;
    /** The hearbead pool controller */
    public static MyNameNode node;
    /** double adpsend */
    public static volatile double adspend;
    /** is the server ready to receive data */
    boolean ready;

    static long deltaTime = 0, deltaWin = 0, deltaClick = 0, deltaPixel = 0, deltaNobid = 0, deltaBid = 0;
    static double qps = 0;
    static double avgx = 0;

    static AtomicLong totalBidTime = new AtomicLong(0);
    static AtomicLong bidCountWindow = new AtomicLong(0);

    /** The JETTY server used by the bidder */
    static Server server;

    static AdminHandler adminHandler;

    /**
     * The bidder's main thread for handling the bidder's activities outside of the
     * JETTY processing
     */
    Thread me;

    /** Trips right before the join with jetty */
    CountDownLatch startedLatch = null;

    /** The campaigns that the bidder is using to make bids with */
    static CampaignSelector campaigns;

    /** Bid target to exchange class map */
    public static Map<String, BidRequest> exchanges = new HashMap<String, BidRequest>();

    /**
     * This is the entry point for the RTB server.
     * 
     * @param args
     *            . String[]. Config file name. If not present, uses default and
     *            port 8080. Options [-s shardkey] [-p port -x sslport]
     * @throws Exception
     *             if the Server could not start (network error, error reading
     *             configuration)
     */
    public static void main(String[] args) {

	String fileName = "Campaigns/payday.json";
	String shard = "";
	Integer port = 8080;
	Integer sslPort = 8081;

	String pidfile = System.getProperty("pidfile");
	if (pidfile != null) {
	    String target = System.getProperty("target");
	    try {
		String pid = "" + Performance.getPid(target);
		Files.write(Paths.get(pidfile), pid.getBytes());
	    } catch (Exception e) {
		System.err.println("WARTNING: Error writing pidfile: " + pidfile);
	    }
	}

	if (args.length == 1)
	    fileName = args[0];
	else {
	    int i = 0;
	    while (i < args.length) {
		switch (args[i]) {
		case "-p":
		    i++;
		    port = Integer.parseInt(args[i]);
		    i++;
		    break;
		case "-s":
		    i++;
		    shard = args[i];
		    i++;
		    break;
		case "-x":
		    i++;
		    sslPort = Integer.parseInt(args[i]);
		    i++;
		    break;
		case "-z":
		    i++;
		    fileName = "zookeeper:" + args[i];
		    i++;
		    break;
		case "-a":
		    i++;
		    fileName = "aerospike:" + args[i];
		    i++;
		    break;
		default:
		    System.out.println("CONFIG FILE: " + args[i]);
		    fileName = args[i];
		    i++;
		    break;
		}
	    }
	}

	try {
	    new RTBServer(fileName, shard, port, sslPort);
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	    System.exit(0);
	}
    }

    /**
     * Class instantiator of the RTBServer.
     * 
     * @param fileName
     *            String. The filename of the configuration file.
     * @throws Exception
     *             if the Server could not start (network error, error reading
     *             configuration)
     */
    public RTBServer(String fileName) throws Exception {

	Configuration.reset(); // this resquired so that when the server is
			       // restarted, the old config won't stick around.
	AddShutdownHook hook = new AddShutdownHook();
	hook.attachShutDownHook();

	config = Configuration.getInstance(fileName);
	campaigns = CampaignSelector.getInstance(); // used to

	kickStart();
    }

    /**
     * Class instantiator of the RTBServer.
     * 
     * @param fileName
     *            String. The filename of the configuration file.
     * @param shard
     *            String. The shard key of this bidder instance.
     * @param port
     *            . int. The port to use for this bidder.
     * @throws Exception
     *             if the Server could not start (network error, error reading
     *             configuration)
     */
    public RTBServer(String fileName, String shard, int port, int sslPort) throws Exception {

	try {
	    Configuration.reset(); // this resquired so that when the server is
				   // restarted, the old config won't stick
				   // around.

	    config = Configuration.getInstance(fileName, shard, port, sslPort);

	    AddShutdownHook hook = new AddShutdownHook();
	    hook.attachShutDownHook();

	    // Controller.getInstance();
	    campaigns = CampaignSelector.getInstance(); // used to
	    kickStart();

	} catch (Exception error) {
	    throw new Exception(error);
	}
    }

    void kickStart() {
	startedLatch = new CountDownLatch(1);
	me = new Thread(this);
	me.start();
	try {
	    startedLatch.await();
	    Thread.sleep(2000);
	    config.testWinUrlWithCache2k();
	} catch (Exception error) {
	    try {
		logger.error("Win Url/Cache2k problem: RTBServer, Fatal error: {}", error.toString());
	    } catch (Exception e) {
		// TODO Auto-generated catch block
		System.out.println("Fatal error: " + error.toString());
	    }
	    me.interrupt();
	}
    }

    /**
     * Returns whether the server has started.
     * 
     * @return boolean. Returns true if ready to start.
     */
    public boolean isReady() {
	return ready;
    }

    public static void panicStop() {
	try {
	    Controller.getInstance().sendShutdown();
	    Thread.sleep(100);
	    logger.info("{anicStop", "Bidder is shutting down *** NOW ****");
	    Controller.getInstance().removeZnode();
	    if (node != null)
		node.stop();

	} catch (Exception e) {
	    e.printStackTrace();
	} catch (Error x) {
	    x.printStackTrace();
	}
    }

    /**
     * Set summary stats.
     */
    public static void setSummaryStats() {
	if (xtime == 0)
	    avgx = 0;
	else
	    avgx = (nobid + bid) / (double) xtime;

	if (System.currentTimeMillis() - deltaTime < 30000)
	    return;

	deltaWin = win - deltaWin;
	deltaClick = clicks - deltaClick;
	deltaPixel = pixels - deltaPixel;
	deltaBid = bid - deltaBid;
	deltaNobid = nobid - deltaNobid;

	qps = (deltaWin + deltaClick + deltaPixel + deltaBid + deltaNobid);
	long secs = (System.currentTimeMillis() - deltaTime) / 1000;
	qps = qps / secs;
	deltaTime = System.currentTimeMillis();
	deltaWin = win;
	deltaClick = clicks;
	deltaPixel = pixels;
	deltaBid = bid;
	deltaNobid = nobid;

	// QPS the exchanges
	BidRequest.getExchangeCounts(secs);
    }

    /**
     * Retrieve a summary of activity.
     * 
     * @return String. JSON based stats of server performance.
     */
    public static String getSummary() throws Exception {
	setSummaryStats();
	Map<String, Object> m = new HashMap<String, Object>();
	m.put("stopped", stopped);
	m.put("loglevel", config.logLevel);
	m.put("ncampaigns", campaigns.size());
	m.put("qps", qps);
	m.put("deltax", avgx);
	m.put("nobidreason", config.printNoBidReason);
	m.put("cpu", Performance.getCpuPerfAsString());
	m.put("memUsed", Performance.getMemoryUsed());
	m.put("cores", Performance.getCores());
	m.put("diskFree", Performance.getPercFreeDisk());
	m.put("openfiles", Performance.getOpenFileDescriptorCount());
	m.put("exchanges", BidRequest.getExchangeCounts());
	m.put("lowonthreads", server.getThreadPool().isLowOnThreads());
	m.put("instance", Configuration.instanceName);

	return DbTools.mapper.writeValueAsString(m);
    }

    /**
     * Establishes the HTTP Handler, creates the Jetty server and attaches the
     * handler and then joins the server. This method does not return, but it is
     * interruptable by calling the halt() method.
     * 
     */
    @Override
    public void run() {

	SSL ssl = config.ssl;
	if (config.port == 0 && ssl == null) {
	    try {
		logger.error("Neither HTTP or HTTPS configured, error, stop");
	    } catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	    return;
	}

	QueuedThreadPool threadPool = new QueuedThreadPool(threads, 50);
	server = new Server(threadPool);
	ServerConnector connector = null;

	if (config.port != 0) {
	    connector = new ServerConnector(server);
	    connector.setPort(config.port);
	    connector.setIdleTimeout(60000);
	}

	if (config.ssl != null) {

	    HttpConfiguration https = new HttpConfiguration();
	    https.addCustomizer(new SecureRequestCustomizer());
	    SslContextFactory sslContextFactory = new SslContextFactory();
	    sslContextFactory.setKeyStorePath(ssl.setKeyStorePath);
	    sslContextFactory.setKeyStorePassword(ssl.setKeyStorePassword);
	    sslContextFactory.setKeyManagerPassword(ssl.setKeyManagerPassword);
	    ServerConnector sslConnector = new ServerConnector(server,
		    new SslConnectionFactory(sslContextFactory, "http/1.1"), new HttpConnectionFactory(https));
	    sslConnector.setPort(config.sslPort);

	    if (connector != null)
		server.setConnectors(new Connector[] { connector, sslConnector });
	    else
		server.setConnectors(new Connector[] { sslConnector });
	    try {
		logger.info("SSL configured on port {}", config.sslPort);
	    } catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	} else
	    server.setConnectors(new Connector[] { connector });

	Handler handler = new Handler();

	node = null;

	try {
	    new WebMQ(7379, null);
	    BidRequest.compile();
	    SessionHandler sh = new SessionHandler(); // org.eclipse.jetty.server.session.SessionHandler
	    sh.setHandler(handler);
	    server.setHandler(sh); // set session handle

	    startPeridocLogger();

	    /**
	     * Override the start state if the deadmanswitch object is not null and the key
	     * doesn't exist
	     */
	    if (config.deadmanSwitch != null) {
		if (config.deadmanSwitch.canRun() == false) {
		    RTBServer.stopped = true;
		}
	    }

	    server.start();

	    Thread.sleep(500);

	    ready = true;
	    deltaTime = System.currentTimeMillis(); // qps timer

	    Controller.getInstance();
	    if (Controller.responseQueue != null) {
		Controller.responseQueue.add(getStatus());
	    }

	    logger.info("********************* System start on port: {}/{}", Performance.getInternalAddress(),
		    config.port);

	    startSeparateAdminServer();

	    startedLatch.countDown();
	    server.join();
	} catch (Exception error) {
	    if (error.toString().contains("Interrupt"))

		try {
		    logger.error("HALT: : {}", error.toString());
		    if (node != null)
			node.halt();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    else
		error.printStackTrace();
	} finally {
	    if (node != null)
		node.stop();
	}
    }

    /**
     * Start a different handler for control and reporting functions
     * 
     * @throws Exception
     *             if SSL is specified but is not configured
     */
    void startSeparateAdminServer() throws Exception {
	SSL ssl = config.ssl;

	QueuedThreadPool threadPool = new QueuedThreadPool(threads, 50);
	Server server = new Server(threadPool);
	ServerConnector connector;

	if (config.adminPort == 0)
	    return;

	logger.info("Admin functions are available on port: {}", config.adminPort);

	if (!config.adminSSL) { // adminPort
	    connector = new ServerConnector(server);
	    connector.setPort(config.adminPort);
	    connector.setIdleTimeout(60000);
	    server.setConnectors(new Connector[] { connector });
	} else {

	    if (config.ssl == null) {
		throw new Exception("Admin port set to SSL but no SSL credentials are configured.");
	    }
	    logger.info("Admin functions are available by SSL only");
	    HttpConfiguration https = new HttpConfiguration();
	    https.addCustomizer(new SecureRequestCustomizer());
	    SslContextFactory sslContextFactory = new SslContextFactory();
	    sslContextFactory.setKeyStorePath(ssl.setKeyStorePath);
	    sslContextFactory.setKeyStorePassword(ssl.setKeyStorePassword);
	    sslContextFactory.setKeyManagerPassword(ssl.setKeyManagerPassword);
	    ServerConnector sslConnector = new ServerConnector(server,
		    new SslConnectionFactory(sslContextFactory, "http/1.1"), new HttpConnectionFactory(https));
	    sslConnector.setPort(config.adminPort);

	    server.setConnectors(new Connector[] { sslConnector });
	}

	adminHandler = new AdminHandler();

	SessionHandler sh = new SessionHandler(); // org.eclipse.jetty.server.session.SessionHandler
	sh.setHandler(adminHandler);
	server.setHandler(sh); // set session handle

	server.start();
	server.join();
    }

    /**
     * Quickie tasks for periodic logging
     */
    void startPeridocLogger() throws Exception {
	if (config.cacheHost != null) {
	    node = new MyNameNode(config.cacheHost, config.cachePort);

	    Runnable redisupdater = () -> {
		try {
		    while (true) {
			Echo e = getStatus();
			Controller.getInstance().setMemberStatus(e);
			Controller.getInstance().updateStatusZooKeeper(e.toJson());

			Controller.getInstance().reportNoBidReasons();

			CampaignProcessor.probe.reset();

			Thread.sleep(ZOOKEEPER_UPDATE);
		    }
		} catch (Exception e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    };
	    Thread nthread = new Thread(redisupdater);
	    nthread.start();
	}

	////////////////////

	Runnable task = () -> {
	    // long count = 0;
	    while (true) {
		try {

		    // RTBServer.paused = true; // for a short time, send
		    // no-bids, this way any
		    // queues needing to drain
		    // have a chance to do so

		    avgBidTime = totalBidTime.get();
		    double davgBidTime = avgBidTime;
		    double window = bidCountWindow.get();
		    if (window == 0)
			window = 1;
		    davgBidTime /= window;

		    String sqps = String.format("%.2f", qps);
		    String savgbidtime = String.format("%.2f", davgBidTime);

		    long a = ForensiqClient.forensiqXtime.get();
		    long b = ForensiqClient.forensiqCount.get();

		    ForensiqClient.forensiqXtime.set(0);
		    ForensiqClient.forensiqCount.set(0);
		    totalBidTime.set(0);
		    bidCountWindow.set(0);

		    server.getThreadPool().isLowOnThreads();
		    if (b == 0)
			b = 1;

		    long avgForensiq = a / b;
		    String perf = Performance.getCpuPerfAsString();
		    int threads = Performance.getThreadCount();
		    String pf = Performance.getPercFreeDisk();
		    String mem = Performance.getMemoryUsed();
		    long of = Performance.getOpenFileDescriptorCount();
		    List<?> exchangeCounts = BidRequest.getExchangeCounts();
		    String msg = "openfiles=" + of + ", cpu=" + perf + "%, mem=" + mem + ", freedsk=" + pf
			    + "%, threads=" + threads + ", low-on-threads= " + server.getThreadPool().isLowOnThreads()
			    + ", qps=" + sqps + ", avgBidTime=" + savgbidtime + "ms, avgForensiq= " + avgForensiq
			    + "ms, total=" + handled + ", requests=" + request + ", bids=" + bid + ", nobids=" + nobid
			    + ", fraud=" + fraud + ", wins=" + win + ", pixels=" + pixels + ", clicks=" + clicks
			    + ", exchanges= " + exchangeCounts + ", stopped=" + stopped + ", campaigns="
			    + config.campaignsList.size();
		    Map<String, Object> m = new HashMap<String, Object>();
		    m.put("timestamp", System.currentTimeMillis());
		    m.put("hostname", Configuration.instanceName);
		    m.put("openfiles", of);
		    m.put("cpu", Double.parseDouble(perf));
		    m.put("bp", Controller.getInstance().getBackPressure());

		    String[] parts = mem.split("M");
		    m.put("memused", Double.parseDouble(parts[0]));
		    parts[1] = parts[1].substring(1, parts[1].length() - 2);
		    parts[1] = parts[1].replaceAll("\\(", "");

		    double percmemused = Double.parseDouble(parts[1]);

		    m.put("percmemused", percmemused);

		    m.put("freedisk", Double.parseDouble(pf));
		    m.put("threads", threads);
		    m.put("qps", qps);
		    m.put("avgbidtime", Double.parseDouble(savgbidtime));
		    m.put("handled", handled);
		    m.put("requests", request);
		    m.put("nobid", nobid);
		    m.put("fraud", fraud);
		    m.put("wins", win);
		    m.put("pixels", pixels);
		    m.put("clicks", clicks);
		    m.put("stopped", stopped);
		    m.put("bids", bid);
		    m.put("exchanges", exchangeCounts);
		    m.put("campaigns", config.campaignsList.size());

		    if (CampaignProcessor.probe != null) {
			// System.out.println("=======> REPORT: " +
			// CampaignProcessor.probe.report());
			m.put("cperform", CampaignProcessor.probe.getMap());
		    }
		    Controller.getInstance().sendStats(m);

		    logger.info("Heartbeat {}", msg);
		    CampaignSelector.adjustHighWaterMark();

		    // Thread.sleep(100);
		    // RTBServer.paused = false;

		    if (percmemused >= 94) {
			logger.error("Memory Usage Exceeded, Exiting");
			Controller.getInstance().sendShutdown();
			System.exit(1);
		    }

		    Thread.sleep(PERIODIC_UPDATE_TIME);

		} catch (Exception e) {
		    e.printStackTrace();
		    return;
		}
	    }
	};
	Thread thread = new Thread(task);
	thread.start();
    }

    /**
     * Returns the sertver's campaign selector used by the bidder. Generally used by
     * javascript programs.
     * 
     * @return CampaignSelector. The campaign selector object used by this server.
     */
    public CampaignSelector getCampaigns() {
	return campaigns;
    }

    /**
     * Stop the RTBServer, this will cause an interrupted exception in the run()
     * method.
     */
    public void halt() {
	try {
	    me.interrupt();
	} catch (Exception error) {
	    System.err.println("Interrupt failed.");
	}
	try {
	    server.stop();
	    while (!server.isStopped())
		Thread.sleep(1);
	} catch (Exception error) {
	    error.printStackTrace();
	}
	try {
	    logger.error("System shutdown");
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    /**
     * Is the Jetty server running and processing HTTP requests?
     * 
     * @return boolean. Returns true if the server is running, otherwise false if
     *         null or isn't running
     */
    public boolean isRunning() {
	if (server == null)
	    return false;

	return server.isRunning();
    }

    /**
     * Returns the status of this server.
     * 
     * @return Map. A map representation of this status.
     */
    public static Echo getStatus() {
	setSummaryStats();
	Echo e = new Echo();

	e.from = Configuration.instanceName;
	e.percentage = percentage.intValue();
	e.stopped = stopped;
	e.request = request;
	e.bid = bid;
	e.win = win;
	e.nobid = nobid;
	e.error = error;
	e.handled = handled;
	e.unknown = unknown;
	e.clicks = clicks;
	e.pixel = pixels;
	e.fraud = fraud;
	e.adspend = adspend;
	e.loglevel = config.logLevel;
	e.qps = qps;
	e.campaigns = config.campaignsList;
	e.avgx = avgx;
	e.exchanges = BidRequest.getExchangeCounts();
	e.timestamp = System.currentTimeMillis();
	if (CampaignProcessor.probe != null) {
	    e.cperform = CampaignProcessor.probe.getMap();
	}

	String perf = Performance.getCpuPerfAsString();
	int threads = Performance.getThreadCount();
	String pf = Performance.getPercFreeDisk();
	String mem = Performance.getMemoryUsed();
	e.threads = threads;
	e.memory = mem;
	e.freeDisk = pf;
	e.cpu = perf;

	return e;
    }
}