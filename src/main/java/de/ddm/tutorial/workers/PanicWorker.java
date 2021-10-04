package de.ddm.tutorial.workers;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class PanicWorker extends AbstractBehavior<PanicWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class StressfulWorkMessage implements Message, Serializable {
		private static final long serialVersionUID = 3323233591421166908L;
		int task;
	}

	@Getter
	@NoArgsConstructor
	public static class PanicMessage implements Message, Serializable {
		private static final long serialVersionUID = 2976676134948559148L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "panicWorker";

	public static Behavior<PanicWorker.Message> create() {
		return Behaviors.setup(context -> new PanicWorker(context));
	}

	private PanicWorker(ActorContext<PanicWorker.Message> context) {
		super(context);
	}

	/////////////////
	// Actor State //
	/////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<PanicWorker.Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StressfulWorkMessage.class, this::handle)
				.onMessage(PanicWorker.PanicMessage.class, this::handle)
				.build();
	}

	private Behavior<PanicWorker.Message> handle(StressfulWorkMessage message) {
		for (int i = 0; i < 5; i++)
			this.getContext().getSelf().tell(new PanicMessage());
		return this;
	}

	private Behavior<PanicWorker.Message> handle(PanicWorker.PanicMessage message) {
		this.getContext().getLog().info("PANIC!");
		return this;
	}
}