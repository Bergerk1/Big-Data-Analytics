package de.ddm.tutorial.workers;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class LazyWorker extends AbstractBehavior<LazyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class LargeWorkMessage implements Message, Serializable {
		private static final long serialVersionUID = -1360802136947505148L;
		int[] tasks;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "lazyWorker";

	public static Behavior<LazyWorker.Message> create(ActorRef<SimpleWorker.Message> worker) {
		return Behaviors.setup(context -> new LazyWorker(worker, context));
	}

	private LazyWorker(ActorRef<SimpleWorker.Message> worker, ActorContext<LazyWorker.Message> context) {
		super(context);
		this.siblingWorker = worker;
		this.childWorker = this.getContext().spawn(SimpleWorker.create(), SimpleWorker.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<SimpleWorker.Message> siblingWorker;
	private final ActorRef<SimpleWorker.Message> childWorker;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<LazyWorker.Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(LargeWorkMessage.class, this::handle)
				.build();
	}

	private Behavior<LazyWorker.Message> handle(LargeWorkMessage message) {
		this.getContext().getLog().info(this.getContext().getSelf().toString());

		for (int task : message.getTasks()) {
			this.siblingWorker.tell(new SimpleWorker.WorkMessage(task));
			this.childWorker.tell(new SimpleWorker.WorkMessage(task));
		}

		this.getContext().getLog().info("Did my work!");

		return this;
	}
}
