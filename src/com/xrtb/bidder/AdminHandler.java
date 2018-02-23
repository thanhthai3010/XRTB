package com.xrtb.bidder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xrtb.commands.Echo;
import com.xrtb.common.Configuration;
import com.xrtb.tools.HeapDumper;

@MultipartConfig
class AdminHandler extends Handler {
    /**
     * The property for temp files.
     */
    private static final MultipartConfigElement MULTI_PART_CONFIG = new MultipartConfigElement(
	    System.getProperty("java.io.tmpdir"));
    // private static final Configuration config = Configuration.getInstance();

    /**
     * The randomizer used for determining to bid when percentage is less than 100
     */
    Random rand = new Random();

    static final Logger logger = LoggerFactory.getLogger(AdminHandler.class);

    @SuppressWarnings("unused")
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
	    throws IOException, ServletException {

	response.addHeader("Access-Control-Allow-Origin", "*");
	response.addHeader("Access-Control-Allow-Headers", "Content-Type");

	InputStream body = request.getInputStream();
	String type = request.getContentType();
	// BidRequest br = null;
	String json = "{}";
	String id = "";
	// Campaign campaign = null;
	// boolean unknown = true;
	RTBServer.handled++;
	// int code = RTBServer.BID_CODE;
	baseRequest.setHandled(true);
	// long time = System.currentTimeMillis();
	boolean isGzip = false;

	response.setHeader("X-INSTANCE", Configuration.instanceName);

	try {
	    if (request.getHeader("Content-Encoding") != null && request.getHeader("Content-Encoding").equals("gzip"))
		isGzip = true;

	    if (target.contains("pinger")) {
		response.setStatus(200);
		response.setContentType("text/html;charset=utf-8");
		baseRequest.setHandled(true);
		response.getWriter().println("OK");
		return;

	    }

	    if (target.contains("info")) {
		response.setContentType("text/javascript;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		Echo e = RTBServer.getStatus();
		String rs = e.toJson();
		response.getWriter().println(rs);
		return;
	    }

	    if (target.contains("checkonthis")) {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		String rs = "<html>" + CampaignProcessor.probe.getTable() + "</html>";
		response.getWriter().println(rs);
		return;
	    }

	    if (target.contains("summary")) {
		response.setContentType("text/javascript;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getWriter().println(RTBServer.getSummary());
		return;
	    }

	    if (target.equals("/status")) {
		baseRequest.setHandled(true);
		response.getWriter().println("OK");
		response.setStatus(200);
		return;
	    }

	    if (target.contains("dump")) {
		String fileName = request.getParameter("filename");
		if (fileName == null) {
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:ss");
		    fileName = sdf.format(new Date()) + ".bin";
		}
		String msg = "Dumped " + fileName;
		try {
		    HeapDumper.dumpHeap(fileName, false);
		} catch (Exception error) {
		    msg = "Error dumping " + fileName + ", error=" + error.toString();
		}
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getWriter().println("<h1>" + msg + "</h1>");
		return;
	    }

	    /**
	     * These are not part of RTB, but are used as part of the simulator and campaign
	     * administrator that sit on the same port as the RTB.
	     */

	    if (type != null && type.contains("multipart/form-data")) {
		try {
		    json = WebCampaign.getInstance().multiPart(baseRequest, request, MULTI_PART_CONFIG);
		    response.setStatus(HttpServletResponse.SC_OK);
		} catch (Exception err) {
		    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		    logger.error("Bad non-bid transaction on multiform reqeues");
		}
		baseRequest.setHandled(true);
		response.getWriter().println(json);
		return;
	    }

	    if (target.contains("favicon")) {
		RTBServer.handled--; // don't count this useless turd.
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getWriter().println("");
		return;
	    }

	    if (target.contains(RTBServer.SIMULATOR_URL)) {
		String page = Charset.defaultCharset()
			.decode(ByteBuffer.wrap(Files.readAllBytes(Paths.get(RTBServer.SIMULATOR_ROOT)))).toString();

		page = Configuration.substitute(page);

		response.setContentType("text/html");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		page = SSI.convert(page);
		response.getWriter().println(page);
		return;
	    }

	    // ///////////////////////////
	    if (target.contains("ajax")) {
		response.setContentType("text/javascript;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		String data = WebCampaign.getInstance().handler(request, body);
		response.getWriter().println(data);
		response.flushBuffer();
		return;
	    }

	    // ///////////////////////////

	    if (target.contains(RTBServer.CAMPAIGN_URL)) {
		String page = Charset.defaultCharset()
			.decode(ByteBuffer.wrap(Files.readAllBytes(Paths.get(RTBServer.CAMPAIGN_ROOT)))).toString();

		page = Configuration.substitute(page);

		response.setContentType("text/html");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		page = SSI.convert(page);
		response.getWriter().println(page);
		return;
	    }

	    if (target.contains(RTBServer.LOGIN_URL)) {
		String page = Charset.defaultCharset()
			.decode(ByteBuffer.wrap(Files.readAllBytes(Paths.get(RTBServer.LOGIN_ROOT)))).toString();

		response.setContentType("text/html");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		page = SSI.convert(page);

		page = Configuration.substitute(page);

		response.getWriter().println(page);
		return;
	    }

	    if (target.contains(RTBServer.ADMIN_URL)) {
		String page = Charset.defaultCharset()
			.decode(ByteBuffer.wrap(Files.readAllBytes(Paths.get(RTBServer.ADMIN_ROOT)))).toString();

		page = SSI.convert(page);
		page = Configuration.substitute(page);

		response.setContentType("text/html");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getWriter().println(page);
		return;
	    }

	    // /////////////////////////////////////////////////////////////////////////////////////////////////////////////

	} catch (

	Exception e) {
	    logger.warn("Bad html processing on {}: {} at line no: {}" + target, e.toString(),
		    Thread.currentThread().getStackTrace()[2].getLineNumber());
	    // if (br != null && br.id.equals("123"))
	    e.printStackTrace();
	    baseRequest.setHandled(true);
	    StringBuilder str = new StringBuilder("{ \"error\":\"");
	    str.append(e.toString());
	    str.append("\", \"file\":\"RTBServer.java\",\"lineno\":");
	    str.append(Thread.currentThread().getStackTrace()[2].getLineNumber());
	    str.append("}");
	    response.setStatus(RTBServer.NOBID_CODE);
	    response.getWriter().println(str.toString());
	    return;
	}

	if (target.startsWith("/rtb/bids")) {
	    response.setStatus(HttpServletResponse.SC_OK);
	    baseRequest.setHandled(true);
	    response.setStatus(RTBServer.NOBID_CODE);
	    return;
	}

	/**
	 * This set of if's handle non bid request transactions.
	 * 
	 */
	try {
	    type = null;
	    /**
	     * Get rid of artifacts coming from embedde urls
	     */
	    if (target.contains("simulator/temp/test") == false)
		target = target.replaceAll("xrtb/simulator/temp/", ""); // load
									// the
									// html
									// test
									// file
									// from
									// here
									// but
									// not
									// resources
	    target = target.replaceAll("xrtb/simulator/", "");

	    // System.out.println("---> ACCESS: " + target + ": " +
	    // getIpAddress(request));
	    if (target.equals("/"))
		target = "/index.html";

	    int x = target.lastIndexOf(".");
	    if (x >= 0) {
		type = target.substring(x);
	    }
	    if (type != null) {
		type = type.toLowerCase().substring(1);
		type = MimeTypes.substitute(type);
		response.setContentType(type);
		File f = new File("./www/" + target);
		if (!f.exists()) {
		    f = new File("./web/" + target);
		    if (!f.exists()) {
			f = new File(target);
			if (!f.exists()) {
			    f = new File("." + target);
			    if (!f.exists()) {
				response.setStatus(HttpServletResponse.SC_NOT_FOUND);
				baseRequest.setHandled(true);
				return;
			    }
			}
		    }
		}

		target = f.getAbsolutePath();
		if (!target.endsWith("html")) {
		    if (target.endsWith("css") || target.endsWith("js")) {
			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			handleJsAndCss(response, f);
			return;
		    }

		    FileInputStream fis = new FileInputStream(f);
		    OutputStream out = response.getOutputStream();

		    // write to out output stream
		    while (true) {
			int bytedata = fis.read();

			if (bytedata == -1) {
			    break;
			}

			try {
			    out.write(bytedata);
			} catch (Exception error) {
			    break; // screw it, pray that it worked....
			}
		    }

		    // flush and close streams.....
		    fis.close();
		    try {
			out.close();
		    } catch (Exception error) {
			System.err.println(""); // don't care
		    }
		    return;
		}

	    }

	    String page = Charset.defaultCharset().decode(ByteBuffer.wrap(Files.readAllBytes(Paths.get(target))))
		    .toString();

	    page = SSI.convert(page);
	    page = Configuration.substitute(page);

	    response.setContentType("text/html");
	    response.setStatus(HttpServletResponse.SC_OK);
	    baseRequest.setHandled(true);
	    sendResponse(response, page);

	} catch (Exception err) {
	    System.out.println("-----> Encounted an unexpected target: '" + target
		    + "' in the admin handler, will return code 200");
	    response.setStatus(HttpServletResponse.SC_OK);
	    baseRequest.setHandled(true);
	}
    }
}
