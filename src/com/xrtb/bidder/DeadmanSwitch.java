package com.xrtb.bidder;

import com.aerospike.client.AerospikeClient;
import com.aerospike.redisson.AerospikeHandler;
import com.aerospike.redisson.RedissonClient;
import com.xrtb.commands.StartBidder;
import com.xrtb.commands.StopBidder;
import com.xrtb.common.Configuration;

/**
 * A class used to stop runaway bidders. If deadmanswitch is set in the startup json, it must be present in the
 * REDIs store before the bidder will bid. The switch is set by the accounting program.
 * @author Ben M. Faul
 *
 */
public class DeadmanSwitch implements Runnable {
	
	RedissonClient redisson;
	Thread me;
	String key;
	boolean sentStop = false;
	public static boolean testmode = false;
	
	
	public DeadmanSwitch(RedissonClient redisson, String key) {
		this.redisson = redisson;
		this.key = key;
		me = new Thread(this);
		me.start();
	}
	
	public DeadmanSwitch(String host, int port, String key) {
		AerospikeHandler spike = AerospikeHandler.getInstance(host,port,300);
		redisson = new RedissonClient(spike);
		
		this.key = key;
		me = new Thread(this);
		me.start();
	}
	
	public DeadmanSwitch() {
		
	}
	
	@Override
	public void run() {
		while(true) {
			try {
				if (canRun() == false) {
					if (sentStop == false) {
						try {
							if (!testmode) {
								Controller.getInstance().sendLog(1, "DeadmanSwitch",
										("Switch error: " + key + ", does not exist, no bidding allowed!"));
								StopBidder cmd = new StopBidder();
								cmd.from = Configuration.getInstance().instanceName;
								Controller.getInstance().stopBidder(cmd);
							} else {
								System.out.println("Deadman Switch is thrown");
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					sentStop = true;
				} else {
					sentStop = false;
					if (RTBServer.stopped) {
						StartBidder cmd = new StartBidder();
							cmd.from = Configuration.getInstance().instanceName;
						try {
							Controller.getInstance().startBidder(cmd);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public String getKey() {
		return key;
	}
	
	public boolean canRun() {
		String value = redisson.get(key);
		if (value == null) {
			return false;
		}
		sentStop = false;
		return true;
	}
}
