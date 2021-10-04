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
import java.util.ArrayList;
import java.util.List;

public class SimpleWorker extends AbstractBehavior<SimpleWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class WorkMessage implements Message, Serializable {
		private static final long serialVersionUID = 7041548741032696121L;
		int task;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RewardMessage implements Message, Serializable {
		private static final long serialVersionUID = -1358811839098055857L;
		int reward;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "simpleWorker";

	public static Behavior<SimpleWorker.Message> create() {
		return Behaviors.setup(context -> new SimpleWorker(context));
	}

	private SimpleWorker(ActorContext<SimpleWorker.Message> context) {
		super(context);
		this.results = new ArrayList<Integer>();
	}

	/////////////////
	// Actor State //
	/////////////////

	private final List<Integer> results;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<SimpleWorker.Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(SimpleWorker.WorkMessage.class, this::handle)
				.onMessage(SimpleWorker.RewardMessage.class, this::handle)
				.build();
	}

	private Behavior<SimpleWorker.Message> handle(SimpleWorker.WorkMessage message) {
		this.getContext().getLog().info(this.getContext().getSelf().toString());

		for (int i = 0; i < message.getTask(); i++)
			this.results.add(i * i);

		this.getContext().getLog().info("Did my work!");

		return this;
	}

	private Behavior<SimpleWorker.Message> handle(SimpleWorker.RewardMessage message) {
		this.getContext().getLog().info(this.getContext().getSelf().toString());

		this.getContext().getLog().info("Thanks for the " + message.getReward() + " reward!");

		return this;
	}
}
