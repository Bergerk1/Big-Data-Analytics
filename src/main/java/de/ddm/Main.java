package de.ddm;

import akka.actor.typed.ActorSystem;
import de.ddm.actors.Guardian;
import de.ddm.configuration.Command;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.singletons.SystemConfigurationSingleton;

import java.io.IOException;

public class Main {

	public static void main(String[] args) {
		Command.applyOn(args);

		SystemConfiguration config = SystemConfigurationSingleton.get();

		final ActorSystem<Guardian.Message> guardian = ActorSystem.create(Guardian.create(), config.getActorSystemName(), config.toAkkaConfig());

		guardian.tell(new Guardian.StartMessage());

		if (config.getRole().equals(SystemConfiguration.MASTER_ROLE)) {
			try {
				System.out.println(">>> Press ENTER to exit <<<");
				System.in.read();
			} catch (IOException ignored) {
			} finally {
				guardian.tell(new Guardian.ShutdownMessage());
			}
		}
	}
}
