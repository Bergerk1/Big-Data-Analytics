package de.ddm.tutorial;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.tutorial.workers.LazyWorker;
import de.ddm.tutorial.workers.PanicWorker;
import de.ddm.tutorial.workers.SimpleWorker;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class TutorialUserGuardian extends AbstractBehavior<TutorialUserGuardian.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message {
	}

	@Data
	@NoArgsConstructor
	public static class StartMessage implements Message, Serializable {
		private static final long serialVersionUID = 7656154615249310784L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static Behavior<Message> create() {
		return Behaviors.setup(context -> new TutorialUserGuardian(context));
	}

	private TutorialUserGuardian(ActorContext<Message> context) {
		super(context);
		this.simpleWorker1 = context.spawn(SimpleWorker.create(), SimpleWorker.DEFAULT_NAME + "1");
		this.simpleWorker2 = context.spawn(SimpleWorker.create(), SimpleWorker.DEFAULT_NAME + "2");
		this.lazyWorker = context.spawn(LazyWorker.create(this.simpleWorker1), LazyWorker.DEFAULT_NAME);
		this.panicWorker = context.spawn(PanicWorker.create(), PanicWorker.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<SimpleWorker.Message> simpleWorker1;
	private final ActorRef<SimpleWorker.Message> simpleWorker2;
	private final ActorRef<LazyWorker.Message> lazyWorker;
	private final ActorRef<PanicWorker.Message> panicWorker;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		this.getContext().getLog().info(this.getContext().getSelf().toString());

		this.simpleWorker1.tell(new SimpleWorker.WorkMessage(42));
		this.simpleWorker2.tell(new SimpleWorker.RewardMessage(8));
		this.lazyWorker.tell(new LazyWorker.LargeWorkMessage(new int[]{1, 2, 3}));
		this.panicWorker.tell(new PanicWorker.StressfulWorkMessage(9001));

		return this;
	}

}
