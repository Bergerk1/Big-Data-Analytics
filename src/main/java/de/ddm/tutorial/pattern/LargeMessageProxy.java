package de.ddm.tutorial.pattern;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.delivery.ConsumerController;
import akka.actor.typed.delivery.ProducerController;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Optional;
import java.util.UUID;

public class LargeMessageProxy extends AbstractBehavior<LargeMessageProxy.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface LargeMessage extends AkkaSerializable {
	}

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class SendMessage implements Message {
		private static final long serialVersionUID = -1203695340601241430L;
		private LargeMessage message;
		private ActorRef<Message> receiverProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class OpenConsumerMessage implements Message {
		private static final long serialVersionUID = 4605052683299507167L;
		private ActorRef<ProducerController.Command<Message>> producerController;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	private static class WrappedLargeMessage implements Message {
		private static final long serialVersionUID = -4555989079577448586L;
		private LargeMessage message;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	private static class WrappedRequestNextMessage implements Message {
		private static final long serialVersionUID = -2623489204213779526L;
		private ProducerController.RequestNext<Message> requestNext;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class WrappedDeliveryMessage implements Message {
		private static final long serialVersionUID = -6901265218517169326L;
		private ConsumerController.Delivery<Message> delivery;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";

	public static Behavior<Message> create(ActorRef<LargeMessage> parent) {
		return Behaviors.setup(context -> new LargeMessageProxy(context, parent));
	}

	private LargeMessageProxy(ActorContext<Message> context, ActorRef<LargeMessage> parent) {
		super(context);

		this.parent = parent;

		this.requestNextAdapter = context.messageAdapter(ProducerController.requestNextClass(), WrappedRequestNextMessage::new);
		this.deliveryAdapter = context.messageAdapter(ConsumerController.deliveryClass(), WrappedDeliveryMessage::new);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessage> parent;

	private final ActorRef<ProducerController.RequestNext<Message>> requestNextAdapter;
	private final ActorRef<ConsumerController.Delivery<Message>> deliveryAdapter;

	private SendMessage message;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(SendMessage.class, this::handle)
				.onMessage(OpenConsumerMessage.class, this::handle)
				.onMessage(WrappedRequestNextMessage.class, this::handle)
				.onMessage(WrappedDeliveryMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(SendMessage message) {

		this.message = message;

		ActorRef<ProducerController.Command<Message>> producerController =
				this.getContext().spawn(ProducerController.create(Message.class, String.valueOf(UUID.randomUUID()), Optional.empty()), "producerController");
		producerController.tell(new ProducerController.Start<>(this.requestNextAdapter));

		message.getReceiverProxy().tell(new LargeMessageProxy.OpenConsumerMessage(producerController));

		return this;
	}

	private Behavior<Message> handle(OpenConsumerMessage message) {

		ActorRef<ConsumerController.Command<Message>> consumerController =
				this.getContext().spawn(ConsumerController.create(), "consumerController");
		consumerController.tell(new ConsumerController.Start<>(this.deliveryAdapter));

		consumerController.tell(new ConsumerController.RegisterToProducerController<>(message.getProducerController()));

		return this;
	}

	private Behavior<Message> handle(WrappedRequestNextMessage message) {

		if (this.message == null)
			return Behaviors.stopped();

		message.getRequestNext().sendNextTo().tell(new WrappedLargeMessage(this.message.getMessage()));
		this.message = null;

		return this;
	}

	private Behavior<Message> handle(WrappedDeliveryMessage message) {

		WrappedLargeMessage wrappedLargeMessage = (WrappedLargeMessage) message.getDelivery().message();

		this.parent.tell(wrappedLargeMessage.getMessage());

		message.getDelivery().confirmTo().tell(ConsumerController.confirmed());

		return Behaviors.stopped();
	}
}
