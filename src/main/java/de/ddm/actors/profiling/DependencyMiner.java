package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.Task;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.lang.reflect.Array;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		String referencedAttribute;
		String dependentAttribute;
		boolean result;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.filesRead = new boolean[this.inputFiles.length];
		this.headersRead = new boolean[this.inputFiles.length];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;
	private final HashMap<String, Set<String>> columnMap = new HashMap<>();
	private final boolean[] filesRead;
	private final boolean[] headersRead;
	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private Stack<Task> tasks;
	private final Map<ActorRef<DependencyWorker.Message>, Task> currentTasks = new HashMap<ActorRef<DependencyWorker.Message>, Task>();
	private int totalTasks = -1;
	private int tasksDone = 0;
	private long dataReadingTime;
	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		this.headersRead[message.getId()] = true;
		this.advanceIfReady();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		// Ignoring batch content for now ... but I could do so much with it.
		if (message.getBatch().size() != 0){
			int id = message.getId();
			System.out.println("Got Batch of File : " + inputFiles[id].toString());
			String filename = inputFiles[id].toString();
			for (int column = 0; column < Array.getLength(headerLines[id]); column++){
				String key = filename.substring(10) + "_" + headerLines[id][column];
				if(!columnMap.containsKey(key))
					columnMap.put(key,new HashSet<>());
				for (int row = 0; row < message.getBatch().size() ; row++) {
					columnMap.get(key).add(message.getBatch().get(row)[column]);
				}
			}
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}else{
			this.filesRead[message.getId()] =true;
			this.advanceIfReady();
		}
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);

			if(readyCheck())
				sendNextTask(dependencyWorker);
				if(tasksDone == totalTasks) end();
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		tasksDone += 1;
		if (message.result) {
			List<InclusionDependency> inds = new ArrayList<>(1);
			inds.add(new InclusionDependency(new File(message.dependentAttribute),new String[] {message.dependentAttribute.substring(message.dependentAttribute.indexOf(".")+5)} , new File(message.referencedAttribute),new String[] {message.referencedAttribute.substring(message.referencedAttribute.indexOf(".")+5)} ));
			this.resultCollector.tell(new ResultCollector.ResultMessage(inds, message.result));
		}
		sendNextTask(dependencyWorker);
		if (tasksDone == totalTasks)
			this.end(); // Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!

		return this;
	}
	private boolean readyCheck(){
		for(boolean b : this.filesRead)
			if(!b) return false;
		for(boolean b : this.headersRead)
			if(!b) return false;
		return true;
	}

	private void advanceIfReady(){
		if(this.readyCheck()){
			dataReadingTime = System.currentTimeMillis() - this.startTime;
			this.getContext().getLog().info("Finished reading data within {} ms!", dataReadingTime);
			Set<String> keys = columnMap.keySet();
			for (String key : keys) {
				System.out.println("Column: "+ key + " with " + columnMap.get(key).size() + " unique entries.");
			}

			tasks = createTasks(keys);
			totalTasks = tasks.size();

			for(ActorRef<DependencyWorker.Message> dependencyWorker : this.dependencyWorkers){
				sendNextTask(dependencyWorker);
			}
			if(tasksDone == totalTasks) end();
		}
	}

	private void sendNextTask(ActorRef<DependencyWorker.Message> dependencyWorker) {
		while(!tasks.empty()){
			Task task = tasks.pop();
			if(task.getNumUniqueDependent()> task.getNumUniqueReferenced()){
				tasksDone += 1;
			}else{
				dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, task,columnMap.get(task.getDependentAttribute()),columnMap.get(task.getReferencedAttribute()))); // send Task from tasklist. What if task fails?
				currentTasks.put(dependencyWorker,task);
				break;
			}
		}
	}

	private Stack<Task> createTasks(Set<String> keys){
		Stack<Task> resultingINDs = new Stack<>();
		for(String key1: keys){
			for(String key2: keys){
				if(!key1.equals(key2)){
					resultingINDs.push(new Task(key1, key2, columnMap.get(key1).size(), columnMap.get(key2).size()));
				}
			}
		}
		return resultingINDs;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms! It took {} ms to read the Data and {} ms to check dependencies.", discoveryTime, dataReadingTime, (discoveryTime- dataReadingTime));
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		tasks.push(currentTasks.get(dependencyWorker)); //put task of dead Worker back to list
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}