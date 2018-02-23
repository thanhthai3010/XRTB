package com.xrtb.bidder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xrtb.common.Configuration;
import com.xrtb.tools.NameNode;

/**
 * This bidder's instance of name node
 * 
 * @author Ben M. Faul
 *
 */
class MyNameNode extends NameNode {
    static final Logger logger = LoggerFactory.getLogger(MyNameNode.class);

    public MyNameNode(String host, int port) throws Exception {
	super(Configuration.instanceName, host, port);
    }

    @Override
    public void log(int level, String where, String msg) {
	try {
	    super.removeYourself();
	    logger.info("{}: {}", where, msg);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

}
