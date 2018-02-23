package com.xrtb.bidder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.redisson.AerospikeHandler;
import com.xrtb.common.Configuration;
import com.xrtb.pojo.BidRequest;
import com.xrtb.pojo.BidResponse;
import com.xrtb.pojo.NobidResponse;
import com.xrtb.pojo.WinObject;

/**
 * JETTY handler for incoming bid request.
 * 
 * This HTTP handler processes RTB2.2 bid requests, win notifications, click
 * notifications, and simulated exchange transactions.
 * <p>
 * Based on the target URI contents, several actions could be taken. A bid
 * request can be processed, a file resource read and returned, a click or pixel
 * notification could be processed.
 * 
 * @author Ben M. Faul
 * 
 */
@MultipartConfig
class Handler extends AbstractHandler {

    private static final CampaignSelector campaignSelector = CampaignSelector.getInstance();

    /**
     * The property for temp files.
     */
    // private static final MultipartConfigElement MULTI_PART_CONFIG = new
    // MultipartConfigElement(
    // System.getProperty("java.io.tmpdir"));
    private static final Configuration config = Configuration.getInstance();

    static final Logger logger = LoggerFactory.getLogger(Handler.class);
    /**
     * The randomizer used for determining to bid when percentage is less than 100
     */
    Random rand = new Random();

    /**
     * Handle the HTTP request. Basically a list of if statements that encapsulate
     * the various HTTP requests to be handled. The server makes no distinction
     * between POST and GET and ignores DELETE>
     * <p>
     * >
     * 
     * @throws IOException
     *             if there is an error reading a resource.
     * @throws ServletException
     *             if the container encounters a servlet problem.
     */
    @SuppressWarnings("unused")
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
	    throws IOException, ServletException {

	response.addHeader("Access-Control-Allow-Origin", "*");
	response.addHeader("Access-Control-Allow-Headers", "Content-Type");

	// get JSON request body data
	InputStream requestBodyData = request.getInputStream();

	// String type = request.getContentType();
	BidRequest bidRequest = null;
	String json = "{}";
	String id = "";
	// Campaign campaign = null;
	boolean unknown = true;
	RTBServer.handled++;
	int code = RTBServer.BID_CODE;
	baseRequest.setHandled(true);
	long time = System.currentTimeMillis();
	boolean isGzip = false;

	response.setHeader("X-INSTANCE", Configuration.instanceName);

	if (request.getHeader("Content-Encoding") != null && request.getHeader("Content-Encoding").equals("gzip"))
	    isGzip = true;

	/*
	 * Uncomment to inspect headers if (1 == 1) { Enumeration headerNames =
	 * request.getHeaderNames(); StringBuilder sb = new
	 * StringBuilder("Header, Target: "); sb.append(target); while
	 * (headerNames.hasMoreElements()) { String headerName = (String)
	 * headerNames.nextElement(); String value = request.getHeader(headerName);
	 * sb.append(headerName); sb.append(": "); sb.append(value); sb.append("\n"); }
	 * try { Controller.getInstance().sendLog(2, "Header Info", sb.toString()); }
	 * catch (Exception e) { } }
	 */

	// System.out.println("------------>" + target);
	/**
	 * This set of if's handle the bid request transactions.
	 */
	BidRequest requestExchange = null;
	try {
	    /**
	     * Convert the uri to a bid request object based on the exchange..
	     */

	    BidResponse bidResponse = null;
	    requestExchange = RTBServer.exchanges.get(target);

	    if (requestExchange != null) {

		if (BidRequest.compilerBusy()) {
		    baseRequest.setHandled(true);
		    response.setHeader("X-REASON", "Server initializing");
		    response.setStatus(RTBServer.NOBID_CODE);
		    return;
		}

		RTBServer.request++;

		/*************
		 * Uncomment to run smaato compliance testing
		 ****************************************/

		/*
		 * Enumeration<String> params = request.getParameterNames(); String tester =
		 * null; if (params.hasMoreElements()) { smaatoCompliance(target, baseRequest,
		 * request, response,body); return;
		 * 
		 * }
		 */

		/************************************************************************************************/

		// if (x == null) {
		// json = "Wrong target: " + target + " is not configured.";
		// code = RTBServer.NOBID_CODE;
		// RTBServer.logger.warn("Handler error: {}", json);
		// RTBServer.error++;
		// System.out.println("=============> Wrong target: " + target + " is not
		// configured.");
		// baseRequest.setHandled(true);
		// response.setStatus(code);
		// response.setHeader("X-REASON", json);
		// response.getWriter().println("{}");
		// return;
		// } else {

		boolean requestLogged = false;
		unknown = false;
		// RunRecord log = new RunRecord("bid-request");

		if (isGzip)
		    requestBodyData = new GZIPInputStream(requestBodyData);

		bidRequest = requestExchange.copy(requestBodyData);
		bidRequest.incrementRequests();

		id = bidRequest.getId();

		if (config.logLevel == -6) {

		    synchronized (Handler.class) {
			dumpRequestInfo(target, request);

			System.out.println(bidRequest.getOriginal());
			RTBServer.nobid++;
			Controller.getInstance().sendNobid(new NobidResponse(bidRequest.id, bidRequest.getExchange()));
			response.setStatus(bidRequest.returnNoBidCode());
			response.setContentType(bidRequest.returnContentType());
			baseRequest.setHandled(true);
			response.setHeader("X-REASON", "debugging");
			return;
		    }
		}

		if (bidRequest.blackListed) {
		    if (bidRequest.id.equals("123") || config.printNoBidReason) {
			logger.info("{} blacklisted", bidRequest.id);
		    }
		    RTBServer.nobid++;
		    Controller.getInstance().sendNobid(new NobidResponse(bidRequest.id, bidRequest.getExchange()));
		    response.setStatus(bidRequest.returnNoBidCode());
		    response.setContentType(bidRequest.returnContentType());
		    response.setHeader("X-REASON", "master-black-list");
		    baseRequest.setHandled(true);
		    bidRequest.writeNoBid(response, time);

		    requestLogged = Controller.getInstance().sendRequest(bidRequest, false);
		    return;
		}

		if (!bidRequest.forensiqPassed()) {
		    code = RTBServer.NOBID_CODE;
		    RTBServer.nobid++;
		    RTBServer.fraud++;
		    Controller.getInstance().sendNobid(new NobidResponse(bidRequest.id, bidRequest.getExchange()));
		    Controller.getInstance().publishFraud(bidRequest.fraudRecord);
		}

		if (RTBServer.server.getThreadPool().isLowOnThreads()) {
		    if (bidRequest.id.equals("123")) {
			logger.warn("Server throttled, low on threads");
		    } else {
			code = RTBServer.NOBID_CODE;
			json = "Server throttling";
			RTBServer.nobid++;
			response.setStatus(bidRequest.returnNoBidCode());
			response.setContentType(bidRequest.returnContentType());
			baseRequest.setHandled(true);
			bidRequest.writeNoBid(response, time);

			Controller.getInstance().sendRequest(bidRequest, false);
			return;
		    }
		}

		if (campaignSelector.size() == 0) {
		    if (bidRequest.id.equals("123")) {
			logger.info("No campaigns loaded");
		    }
		    json = BidRequest.returnNoBid("No campaigns loaded");
		    code = RTBServer.NOBID_CODE;
		    RTBServer.nobid++;
		    Controller.getInstance().sendRequest(bidRequest, false);
		    Controller.getInstance().sendNobid(new NobidResponse(bidRequest.id, bidRequest.getExchange()));
		} else if (RTBServer.stopped || RTBServer.paused) {
		    if (bidRequest.id.equals("123")) {
			logger.info("Server stopped");
		    }
		    json = BidRequest.returnNoBid("Server stopped");
		    code = RTBServer.NOBID_CODE;
		    RTBServer.nobid++;
		    Controller.getInstance().sendNobid(new NobidResponse(bidRequest.id, bidRequest.getExchange()));
		} else if (!checkPercentage()) {
		    json = BidRequest.returnNoBid("Server throttled");
		    if (bidRequest.id.equals("123")) {
			logger.info("Percentage throttled");
		    }
		    code = RTBServer.NOBID_CODE;
		    RTBServer.nobid++;
		    Controller.getInstance().sendNobid(new NobidResponse(bidRequest.id, bidRequest.getExchange()));
		} else {
		    // if (RTBServer.strategy ==
		    // Configuration.STRATEGY_HEURISTIC)
		    // bresp =
		    // CampaignSelector.getInstance().getHeuristic(br); //
		    // 93%
		    // time
		    // here
		    // else

		    // Some exchanges like Appnexus send other endpoints, so
		    // they are handled here.
		    if (bidRequest.notABidRequest()) {
			code = bidRequest.getNonBidReturnCode();
			json = bidRequest.getNonBidRespose();
		    } else {

			// Campaign Select
			bidResponse = campaignSelector.getMaxConnections(bidRequest);

			// log.add("select");
			if (bidResponse == null) {
			    code = RTBServer.NOBID_CODE;
			    json = BidRequest.returnNoBid("No matching campaign");
			    code = RTBServer.NOBID_CODE;
			    RTBServer.nobid++;
			    Controller.getInstance().sendRequest(bidRequest, false);
			    Controller.getInstance()
				    .sendNobid(new NobidResponse(bidRequest.id, bidRequest.getExchange()));
			} else {
			    code = RTBServer.BID_CODE;
			    if (!bidResponse.isNoBid()) {
				bidRequest.incrementBids();
				// if (Configuration.requstLogStrategy == Configuration.REQUEST_STRATEGY_BIDS)
				// Controller.getInstance().sendRequest(br);
				Controller.getInstance().sendBid(bidRequest, bidResponse);
				Controller.getInstance().recordBid(bidResponse);
				// System.out.println("\t->D + " + requestLogged);
				if (!requestLogged)
				    Controller.getInstance().sendRequest(bidRequest, true);
				// System.out.println("\t->E");
				RTBServer.bid++;

			    }
			}
		    }
		}
		// log.dump();
		// }

		time = System.currentTimeMillis() - time;

		response.setHeader("X-TIME", Long.toString(time));
		RTBServer.xtime += time;

		response.setContentType(bidRequest.returnContentType()); // "application/json;charset=utf-8");
		if (code == 204) {
		    response.setHeader("X-REASON", json);
		    if (config.printNoBidReason)
			System.out.println("No bid: " + json);
		    response.setStatus(bidRequest.returnNoBidCode());
		}

		baseRequest.setHandled(true);

		if (code == 200) {
		    RTBServer.totalBidTime.addAndGet(time);
		    RTBServer.bidCountWindow.incrementAndGet();
		    response.setStatus(code);
		    if (bidResponse != null)
			bidResponse.writeTo(response);
		} else {
		    bidRequest.writeNoBid(response, time);
		}
		return;
	    }

	    // //////////////////////////////////////////////////////////////////////
	    if (target.contains("/rtb/win")) {
		StringBuffer url = request.getRequestURL();
		String queryString = request.getQueryString();
		response.setStatus(HttpServletResponse.SC_OK);
		json = "";
		if (queryString != null) {
		    url.append('?');
		    url.append(queryString);
		}
		String requestURL = url.toString();

		try {
		    json = WinObject.getJson(requestURL);
		    if (json == null) {

		    }
		    RTBServer.win++;
		} catch (Exception error) {
		    response.setHeader("X-ERROR", "Error processing win response");
		    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		    logger.warn("Bad win response {}", requestURL);
		    error.printStackTrace();
		}
		response.setContentType("text/html;charset=utf-8");
		baseRequest.setHandled(true);
		response.getWriter().println(json);
		return;
	    }

	    if (target.contains("/pixel")) {
		Controller.getInstance().publishPixel(target);
		response.setContentType("image/bmp;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getWriter().println("");
		RTBServer.pixels++;
		return;
	    }

	    if (target.contains("/redirect")) {
		Controller.getInstance().publishClick(target);
		StringBuffer url = request.getRequestURL();
		String queryString = request.getQueryString();
		String params[] = null;
		if (queryString != null)
		    params = queryString.split("url=");

		baseRequest.setHandled(true);

		if (params != null) {
		    response.sendRedirect(URLDecoder.decode(params[1], "UTF-8"));
		}
		RTBServer.clicks++;
		return;
	    }

	    if (target.contains("pinger")) {
		response.setStatus(200);
		response.setContentType("text/html;charset=utf-8");
		baseRequest.setHandled(true);
		response.getWriter().println("OK");
		return;

	    }

	    if (target.contains("summary")) {
		response.setContentType("text/javascript;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getWriter().println(RTBServer.getSummary());
		return;
	    }

	    if (target.contains("favicon")) {
		RTBServer.handled--; // don't count this useless turd.
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getWriter().println("");
		return;
	    }

	    if (RTBServer.adminHandler != null) {
		baseRequest.setHandled(true);
		response.setStatus(404);
		logger.warn("Error: wrong request for admin login: {}, target: {}", getIpAddress(request), target);
		RTBServer.error++;
	    } else {
		AdminHandler admin = new AdminHandler();
		admin.handle(target, baseRequest, request, response);
		return;
	    }
	} catch (Exception error) {
	    // error.printStackTrace(); // TBD TO SEE THE ERRORS

	    /////////////////////////////////////////////////////////////////////////////
	    // If it's an aerospike error, see ya!
	    //
	    if (error.toString().contains("Parse")) {
		if (bidRequest != null) {
		    bidRequest.incrementErrors();
		    try {
			logger.error("Error: Bad JSON from {}: {} ", bidRequest.getExchange(), error.toString());
		    } catch (Exception e) {
			e.printStackTrace();
		    }
		}
	    }
	    if (error.toString().contains("Aerospike")) {
		AerospikeHandler.reset();
	    }
	    ////////////////////////////////////////////////////////////////////////////

	    RTBServer.error++;
	    String exchange = target;
	    if (requestExchange != null) {
		requestExchange.incrementErrors();
		exchange = requestExchange.getExchange();
	    }
	    StringWriter errors = new StringWriter();
	    error.printStackTrace(new PrintWriter(errors));
	    if (errors.toString().contains("fasterxml")) {
		logger.debug("Handler:handle", "Error: bad JSON data from {}: {}", exchange, error.toString());
	    } // else
	      // error.printStackTrace();
	    response.setStatus(RTBServer.NOBID_CODE);
	}
    }

    void handleJsAndCss(HttpServletResponse response, File file) throws Exception {
	byte fileContent[] = new byte[(int) file.length()];
	FileInputStream fin = new FileInputStream(file);
	int rc = fin.read(fileContent);
	if (rc != fileContent.length) {
	    fin.close();
	    throw new Exception("Incomplete read of " + file.getName());
	}

	fin.close();
	sendResponse(response, new String(fileContent));
    }

    public static void sendResponse(HttpServletResponse response, String html) throws Exception {

	try {
	    byte[] bytes = compressGZip(html);
	    response.addHeader("Content-Encoding", "gzip");
	    int sz = bytes.length;
	    response.setContentLength(sz);
	    response.getOutputStream().write(bytes);

	} catch (Exception e) {
	    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	    response.getOutputStream().println("");
	    e.printStackTrace();
	}
    }

    @SuppressWarnings("unused")
    private static String uncompressGzip(InputStream stream) throws Exception {
	ByteArrayOutputStream baos = new ByteArrayOutputStream();

	GZIPInputStream gzis = new GZIPInputStream(stream);
	byte[] buffer = new byte[1024];
	int len = 0;
	String str = "";

	while ((len = gzis.read(buffer)) > 0) {
	    str += new String(buffer, 0, len);
	}

	gzis.close();
	return str;
    }

    private static byte[] compressGZip(String uncompressed) throws Exception {
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	GZIPOutputStream gzos = new GZIPOutputStream(baos);

	byte[] uncompressedBytes = uncompressed.getBytes();

	gzos.write(uncompressedBytes, 0, uncompressedBytes.length);
	gzos.close();

	return baos.toByteArray();
    }

    private void dumpRequestInfo(String target, HttpServletRequest req) {
	int level = config.logLevel;
	if (level != -6)
	    return;

	Enumeration<String> headerNames = req.getHeaderNames();
	System.out.println("============================");
	System.out.println("Target: " + target);
	System.out.println("IP = " + getIpAddress(req));
	System.out.println("Headers:");
	while (headerNames.hasMoreElements()) {
	    String headerName = headerNames.nextElement();
	    Enumeration<String> headers = req.getHeaders(headerName);
	    System.out.print(headerName + " = ");
	    while (headers.hasMoreElements()) {
		String headerValue = headers.nextElement();
		System.out.print(headerValue);
		if (headers.hasMoreElements())
		    System.out.print(", ");
	    }
	    System.out.println();
	}
	System.out.println("----------------------------");
    }

    @SuppressWarnings({ "unused", "null" })
    private void smaatoCompliance(String target, Request baseRequest, HttpServletRequest request,
	    HttpServletResponse response, InputStream body) throws Exception {
	String tester = null;
	String json = null;
	BidRequest br = null;

	Enumeration<String> params = request.getParameterNames();
	if (params.hasMoreElements()) {
	    String[] dobid = request.getParameterValues(params.nextElement());
	    tester = dobid[0];
	    System.out.println("=================> SMAATO TEST ====================");
	}

	if (tester == null) {
	    System.out.println("              Nothing to Test");
	    return;
	}

	if (tester.equals("nobid")) {
	    RTBServer.nobid++;
	    baseRequest.setHandled(true);
	    response.setStatus(RTBServer.NOBID_CODE);
	    response.getWriter().println("");
	    logger.info("SMAATO NO BID TEST ENDPOINT REACHED");
	    Controller.getInstance().sendNobid(new NobidResponse(br.id, br.getExchange()));
	    return;
	} else {
	    BidRequest x = RTBServer.exchanges.get(target);
	    x.setExchange("nexage");
	    br = x.copy(body);

	    Controller.getInstance().sendRequest(br, false);

	    logger.info("SMAATO MANDATORY BID TEST ENDPOINT REACHED");
	    BidResponse bresp = null;
	    // if (RTBServer.strategy == Configuration.STRATEGY_HEURISTIC)
	    // bresp = CampaignSelector.getInstance().getHeuristic(br); // 93%
	    // time
	    // here
	    // else
	    bresp = campaignSelector.getMaxConnections(br);
	    // log.add("select");
	    if (bresp == null) {
		baseRequest.setHandled(true);
		response.setStatus(RTBServer.NOBID_CODE);
		response.getWriter().println("");
		logger.error("Handler:handle", "SMAATO FORCED BID TEST ENDPOINT FAILED");
		Controller.getInstance().sendNobid(new NobidResponse(br.id, br.getExchange()));
		return;
	    }
	    json = bresp.toString();
	    baseRequest.setHandled(true);
	    Controller.getInstance().sendBid(br, bresp);
	    Controller.getInstance().recordBid(bresp);
	    RTBServer.bid++;
	    response.setStatus(RTBServer.BID_CODE);

	    response.getWriter().println(json);

	    System.out.println("+++++++++++++++++++++ SMAATO REQUEST ++++++++++++++++++++++\n\n" +

		    br.toString() +

		    "\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

	    System.out.println("===================== SMAATO BID ==========================\n\n" + json
		    + "\n\n==========================================================");

	    logger.info("SMAATO FORCED BID TEST ENDPOINT REACHED OK");
	    return;
	}
	/************************************************************************************/
    }

    /**
     * Checks to see if the bidder wants to bid on only a certain percentage of bid
     * requests coming in - a form of throttling.
     * <p>
     * If percentage is set to .20 then twenty percent of the bid requests will be
     * rejected with a NO-BID return on 20% of all traffic received by the Handler.
     * 
     * @return boolean. True means try to bid, False means don't bid
     */
    boolean checkPercentage() {
	if (RTBServer.percentage.intValue() == 100)
	    return true;
	int x = rand.nextInt(101);
	return x < RTBServer.percentage.intValue();
    }

    /**
     * Return the IP address of this
     * 
     * @param request
     *            HttpServletRequest. The web browser's request object.
     * @return String the ip:remote-port of this browswer's connection.
     */
    public String getIpAddress(HttpServletRequest request) {
	String ip = request.getHeader("x-forwarded-for");
	if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
	    ip = request.getHeader("Proxy-Client-IP");
	}
	if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
	    ip = request.getHeader("WL-Proxy-Client-IP");
	}
	if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
	    ip = request.getRemoteAddr();
	}
	ip += ":" + request.getRemotePort();
	return ip;
    }
}
