package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.events.EventConsumer;
import edu.stevens.cs549.dht.events.EventProducer;
import edu.stevens.cs549.dht.events.IEventListener;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceBlockingStub;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceStub;
import edu.stevens.cs549.dht.state.IChannels;
import edu.stevens.cs549.dht.state.IState;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebClient {
	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private IChannels channels;

	private WebClient(IChannels channels) {
		this.channels = channels;
	}

	public static WebClient getInstance(IState state) {
		return new WebClient(state.getChannels());
	}

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}

	/*
	 * Get a blocking stub (channels and stubs are cached for reuse).
	 */
	private DhtServiceBlockingStub getStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newBlockingStub(channel);
	}

	private DhtServiceBlockingStub getStub(NodeInfo target) throws DhtBase.Failed {
		return getStub(target.getHost(), target.getPort());
	}

	private DhtServiceStub getListenerStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newStub(channel);
	}

	private DhtServiceStub getListenerStub(NodeInfo target) throws DhtBase.Failed {
		return getListenerStub(target.getHost(), target.getPort());
	}


	/*
	 * TODOO DONE MAYBE: Fill in missing operations.
	 */

	/*
	 * Get the predecessor pointer at a node.
	 */
	public OptNodeInfo getPred(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "getPred("+node.getId()+")");
		return getStub(node).getPred(Empty.getDefaultInstance());
	}
	public NodeInfo getSucc(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "getSucc("+node.getId()+")");
		return getStub(node).getSucc(Empty.getDefaultInstance());
	}
	public NodeInfo closestPrecedingFinger(NodeInfo info, int id) throws DhtBase.Failed {
		Id protoId = Id.newBuilder().setId(id).build();
		return getStub(info).closestPrecedingFinger(protoId);
	}

	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public OptNodeBindings notify(NodeInfo node, NodeBindings predDb) throws DhtBase.Failed {
		Log.weblog(TAG, "notify("+node.getId()+")");
		// TODOO DONE MAYBE
//		throw new IllegalStateException("notify() not yet implemented");
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
		try {
			OptNodeBindings bindings = getStub(node).notify(predDb);
			return bindings;
		} catch (Exception e) {
			logger.log(Level.WARNING, e.getMessage(), e);
		}
		return null;
	}

	public String[] getBindings(NodeInfo n, String k) throws DhtBase.Failed {

		return getStub(n).getBindings(Key.newBuilder().setKey(k).build()).getValueList().toArray(String[]::new);
	}

	public void addBinding(NodeInfo n, String k, String v) throws DhtBase.Failed {
		getStub(n).addBinding(Binding.newBuilder().setKey(k).setValue(v).build());
	}

	public void deleteBinding(NodeInfo n, String k, String v) throws DhtBase.Failed {
		getStub(n).deleteBinding(Binding.newBuilder().setKey(k).setValue(v).build());
	}

	public NodeInfo findSuccessor(String host, int port, int id) throws DhtBase.Failed {
		return getStub(host, port).findSuccessor(Id.newBuilder().setId(id).build());
	}

	/*
	 * Listening for new bindings.
	 */
	public void listenOn(NodeInfo node, Subscription subscription, IEventListener listener) throws DhtBase.Failed {
		Log.weblog(TAG, "listenOn("+node.getId()+")");
		// TODO listen for updates for the key specified in the subscription
//		getListenerStub(node).listenOn(subscription, EventConsumer.create(subscription.getKey(), listener));

	}

	public void listenOff(NodeInfo node, Subscription subscription) throws DhtBase.Failed {
		Log.weblog(TAG, "listenOff("+node.getId()+")");
		// TODO stop listening for updates on bindings to the key in the subscription
//		getStub(node).listenOff(subscription);
	}
	
}
