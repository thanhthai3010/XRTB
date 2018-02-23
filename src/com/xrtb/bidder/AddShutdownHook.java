package com.xrtb.bidder;

class AddShutdownHook {
    public void attachShutDownHook() {
	Runtime.getRuntime().addShutdownHook(new Thread() {
	    @Override
	    public void run() {
		RTBServer.panicStop();
	    }
	});
	RTBServer.logger.info("*** Shut Down Hook Attached. ***");
    }
}
