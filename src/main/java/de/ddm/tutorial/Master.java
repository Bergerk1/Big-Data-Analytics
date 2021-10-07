package de.ddm.tutorial;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.typed.ClusterSingleton;
import akka.cluster.typed.ClusterSingletonSettings;
import akka.cluster.typed.SingletonActor;
import de.ddm.actors.Guardian;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.actors.profiling.InputReader;
import de.ddm.actors.profiling.ResultCollector;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.InclusionDependency;
import de.ddm.tutorial.workers.SimpleWorker;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.time.Duration;
import java.util.*;

public class Master extends AbstractBehavior<Master.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> worker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> worker;
		int result;
	}

	private class Task {
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";

	public static Behavior<Message> create() {
		return Behaviors.setup(Master::new);
	}

	private Master(ActorContext<Message> context) {
		super(context);
	}

	/////////////////
	// Actor State //
	/////////////////

	private ActorRef<ResultCollector.Message> resultCollector;

	private final Queue<Task> unassignedTasks = new LinkedList<>();
	private final Queue<ActorRef<DependencyWorker.Message>> idleWorkers = new LinkedList<>();
	private final Map<ActorRef<DependencyWorker.Message>, Task> busyWorkers = new HashMap<>();

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> worker = message.getWorker();

		if (this.busyWorkers.containsKey(worker) || this.idleWorkers.contains(worker))
			return this;

		this.getContext().watch(worker);

		if (this.unassignedTasks.isEmpty()) {
			this.idleWorkers.add(worker);
			return this;
		}

		Task task = this.unassignedTasks.remove();
		this.busyWorkers.put(worker, task);
//		worker.tell(new DependencyWorker.TaskMessage(task));
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> worker = message.getWorker();

//		this.resultCollector.tell(new ResultCollector.ResultMessage(message.getResult()));

		if (this.unassignedTasks.isEmpty()) {
			this.idleWorkers.add(worker);
			return this;
		}

		Task task = this.unassignedTasks.remove();
		this.busyWorkers.put(worker, task);
//		worker.tell(new DependencyWorker.TaskMessage(work));
		return this;
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> terminatedWorker = signal.getRef().unsafeUpcast();

		if (this.idleWorkers.remove(terminatedWorker))
			return this;

		Task task = this.busyWorkers.remove(terminatedWorker);

		if (this.idleWorkers.isEmpty()) {
			this.unassignedTasks.add(task);
			return this;
		}

		ActorRef<DependencyWorker.Message> worker = this.idleWorkers.remove();
		this.busyWorkers.put(worker, task);
//		worker.tell(new DependencyWorker.TaskMessage(work));
		return this;
	}
}