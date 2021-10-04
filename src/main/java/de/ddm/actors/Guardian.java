package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.Reaper;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.NoArgsConstructor;

public class Guardian extends AbstractBehavior<Guardian.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -6896669928271349802L;
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final ServiceKey<Guardian.Message> masterService = ServiceKey.create(Guardian.Message.class, "masterService");
	public static final ServiceKey<Guardian.Message> workerService = ServiceKey.create(Guardian.Message.class, "workerService");

	public static Behavior<Message> create() {
		return Behaviors.setup(Guardian::new);
	}

	private Guardian(ActorContext<Message> context) {
		super(context);

		this.reaper = context.spawn(Reaper.create(), Reaper.DEFAULT_NAME);

		switch (SystemConfigurationSingleton.get().getRole()) {
			case SystemConfiguration.MASTER_ROLE:
				this.master = context.spawn(Master.create(), Master.DEFAULT_NAME);
				context.getSystem().receptionist().tell(Receptionist.register(masterService, context.getSelf()));
				break;
			case SystemConfiguration.WORKER_ROLE:
				this.master = null;
				context.getSystem().receptionist().tell(Receptionist.register(workerService, context.getSelf()));
				break;
			default:
				throw new RuntimeException("Unexpected role defined: " + SystemConfigurationSingleton.get().getRole());
		}

		this.worker = context.spawn(Worker.create(), Worker.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<Reaper.Message> reaper;
	private final ActorRef<Master.Message> master;
	private final ActorRef<Worker.Message> worker;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		if (this.master != null)
			this.master.tell(new Master.StartMessage());
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		if (this.worker != null)
			this.worker.tell(new Worker.ShutdownMessage());
		if (this.master != null)
			this.master.tell(new Master.ShutdownMessage());
		return this;
	}
}