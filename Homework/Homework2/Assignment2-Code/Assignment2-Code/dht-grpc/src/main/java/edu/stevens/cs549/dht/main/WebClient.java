package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.rpc.Binding;
import edu.stevens.cs549.dht.rpc.Bindings;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc;
import edu.stevens.cs549.dht.rpc.Id;
import edu.stevens.cs549.dht.rpc.Key;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.rpc.OptNodeBindings;
import edu.stevens.cs549.dht.rpc.OptNodeInfo;
import edu.stevens.cs549.dht.state.IChannels;
import edu.stevens.cs549.dht.state.IState;
import io.grpc.*;

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
	private DhtServiceGrpc.DhtServiceBlockingStub getStub(String targetHost, int targetPort) {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newBlockingStub(channel);
	}

	private DhtServiceGrpc.DhtServiceBlockingStub getStub(NodeInfo target) {
		return getStub(target.getHost(), target.getPort());
	}



	/*
	 * Get the predecessor pointer at a node.
	 */
	public OptNodeInfo getPred(NodeInfo node) {
		Log.weblog(TAG, "getPred("+node.getId()+")");
		return getStub(node).getPred(Empty.getDefaultInstance());
	}


	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public OptNodeBindings notify(NodeInfo node, NodeBindings predDb) throws DhtBase.Failed {
		// TODO
		// throw new IllegalStateException("notify() not yet implemented");
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null.
		 */

		try {
			return getStub(node).notify(predDb);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}


	/*
	 * TODO: Fill in missing operations.
	 */

	public NodeInfo getSucc(NodeInfo info) {
		return getStub(info).getSucc(Empty.getDefaultInstance());
	}

	public NodeInfo closestPrecedingFinger(NodeInfo info, int id) {
		Id protoId = Id.newBuilder().setId(id).build();
		return getStub(info).closestPrecedingFinger(protoId);
	}

	public String[] get(NodeInfo n, String k) {

		return getStub(n).getBindings(Key.newBuilder().setKey(k).build()).getValueList().toArray(new String[0]);
	}

	public void add(NodeInfo n, String k, String v) {
		getStub(n).addBinding(Binding.newBuilder().setKey(k).setValue(v).build());
	}

	public void delete(NodeInfo n, String k, String v) {
		getStub(n).deleteBinding(Binding.newBuilder().setKey(k).setValue(v).build());
	}
//	private DhtServiceGrpc.DhtServiceBlockingStub createRemoteStub(String host, int port) {

//		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
//				.usePlaintext() // Use plaintext for simplicity; consider TLS for production
//				.build();
//		return DhtServiceGrpc.newBlockingStub(channel);
//	}
	public NodeInfo findSuccessor(String host, int port, int id) {
		DhtServiceGrpc.DhtServiceBlockingStub remoteStub = getStub(host, port);
		return remoteStub.findSuccessor(Id.newBuilder().setId(id).build());
	}
}
