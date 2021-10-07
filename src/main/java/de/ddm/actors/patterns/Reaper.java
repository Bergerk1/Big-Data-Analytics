package de.ddm.actors.patterns;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.Guardian;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.ReaperSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class Reaper extends AbstractBehavior<Reaper.Message> {

	public static <T> void watchWithDefaultReaper(ActorRef<T> actor) {
		ReaperSingleton.get().tell(new WatchMeMessage(actor.unsafeUpcast()));
	}

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class WatchMeMessage implements Message {
		private static final long serialVersionUID = 2674402496050807748L;
		private ActorRef<Void> actor;
	}

	@NoArgsConstructor
	public static class EndTheWorldMessage implements Message {
		private static final long serialVersionUID = -8430537933942160149L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = 2336368568740749020L;
		Receptionist.Listing listing;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "reaper";

	public static Behavior<Message> create() {
		return Behaviors.setup(
				context -> Behaviors.withTimers(timers -> new Reaper(context, timers)));
	}

	private Reaper(ActorContext<Message> context, TimerScheduler<Message> timers) {
		super(context);

		this.timers = timers;

		ReaperSingleton.set(this.getContext().getSelf());

		if (SystemConfigurationSingleton.get().getRole().equals(SystemConfiguration.MASTER_ROLE)) {
			final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
			context.getSystem().receptionist().tell(Receptionist.subscribe(Guardian.workerService, listingResponseAdapter));
		}
	}

	/////////////////
	// Actor State //
	/////////////////

	private final TimerScheduler<Message> timers;

	private boolean isShutdown = false;

	private final Set<ActorRef<Void>> watchees = new HashSet<>();
	private Set<ActorRef<Guardian.Message>> workerUserGuardians = new HashSet<>();

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(WatchMeMessage.class, this::handle)
				.onMessage(EndTheWorldMessage.class, this::handle)
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(WatchMeMessage message) {
		this.getContext().getLog().info("Watching " + message.getActor().path().name());

		if (this.watchees.add(message.getActor()))
			this.getContext().watch(message.getActor());
		return this;
	}

	private Behavior<Message> handle(EndTheWorldMessage message) {
		this.timers.cancelAll();
		this.getContext().getSystem().terminate();
		return Behaviors.stopped();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		this.workerUserGuardians = message.getListing().getServiceInstances(Guardian.workerService);

		if (this.workerUserGuardians.isEmpty() && this.isShutdown)
			this.getContext().getSelf().tell(new EndTheWorldMessage());

		return this;
	}

	private Behavior<Message> handle(Terminated signal) {
		final ActorRef<Void> actor = signal.getRef();

		if (!this.watchees.remove(actor)) {
			this.getContext().getLog().error("Received termination signal from unwatched {}.", actor);
			return this;
		}

		if (!this.watchees.isEmpty())
			return this;

		this.getContext().getLog().info("Every local actor has been reaped. Terminating the actor system...");

		if (this.workerUserGuardians.isEmpty()) {
			this.getContext().getSelf().tell(new EndTheWorldMessage());
			return this;
		}

		this.isShutdown = true;

		for (ActorRef<Guardian.Message> workerUserGuardian : this.workerUserGuardians)
			workerUserGuardian.tell(new Guardian.ShutdownMessage());

		this.timers.startTimerAtFixedRate("EndTheWorld", new EndTheWorldMessage(), Duration.ofSeconds(10), Duration.ofSeconds(5));
		return this;
	}
}
