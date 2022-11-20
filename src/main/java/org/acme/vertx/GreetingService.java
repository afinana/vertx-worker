package org.acme.vertx;

import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class GreetingService {

	@Inject
	@RestClient
	RestClientService service;
	
	@Inject ManagedExecutor executor;

	@ConsumeEvent(value = "greeting", blocking = false)
    public String onEvent(String name) {
    
    	log.info("@ConsumeEvent. param:"+name);
    	//return Uni.createFrom().item(() -> greeting(name)).emitOn(executor);
    	
    	Uni.createFrom()
        .item(UUID::randomUUID)
        .emitOn(Infrastructure.getDefaultWorkerPool())
        .subscribe()
        .with(this::worker, Throwable::printStackTrace);
    	
    	 // Uni example:
    	//  function:  public Uni<String> onEvent(String name)        
    	 // return Uni.createFrom().item(() ->  greeting(name)).emitOn(executor);
    	
    	return "OK";

    	
    }

	private Uni<Void> worker(UUID uuid) {
		
        log.info("Starting work: " + uuid);
        try {
        	greeting(uuid.toString());
        } catch (Exception ex) {
            log.info("Could not finish work: " + uuid);
            throw new RuntimeException(ex);
        }
        log.info("Finish work: {}.", uuid);
       
        return Uni.createFrom().voidItem();
    }
 

	private void sleep() {
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			log.error("greeting",e);
		}
	}
    
    public String greeting(String name) {
        
    	sleep();
    	int length = service.getById("test").length();
		log.info("html output:"+length);
    	return "Event: " + name+ ".length output:"+length;
    }

} 