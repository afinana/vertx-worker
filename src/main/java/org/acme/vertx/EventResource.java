package org.acme.vertx;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.jboss.logging.MDC;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.mutiny.core.eventbus.EventBus;
import io.vertx.mutiny.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;

@Path("/async")
@Slf4j
@ApplicationScoped
public class EventResource implements QuarkusApplication{

	private static final int WORKERS_NUM = 5;

	CountDownLatch latch = new CountDownLatch(WORKERS_NUM);
	
	@Inject
	EventBus bus;

	@Inject
	Vertx vertx;

	WorkerExecutor executor;

	@Inject
	GreetingService service;

	@PostConstruct
	void init() {
		this.executor = vertx.createSharedWorkerExecutor("my-worker", 10);
	}


	@GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("{name}")
    public Uni<String> greeting(String name) {
        return bus.<String>request("greeting", name)            
                .onItem().transform(Message::body);            
    }

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/worker1/{name}")
	public String worker1(String name) {
		for (int i = 0; i < 5; i++) {
			workerExecutor1(vertx, i);
		}
		return "OK1";
	}

	
	// FAIL : Doesn't create a new thread
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/worker2/{name}")
	public String worker2(String name) {
		for (int i = 0; i < 5; i++) {
			workerExecutor2(vertx, i);
		}
		return "OK2";
	}

	// RUN OK
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/worker_uni/{name}")
	public String workerUni(String name) {
		for (int i = 0; i < 5; i++) {
			workerExecutorUni(i);
		}
		return "OK_UNI";
	}

	public void workerExecutor1(Vertx vertx, int i) {
		// WorkerExecutor executor = vertx.createSharedWorkerExecutor("my-worker-pool");
		executor.executeBlocking(future -> {
			// Call some blocking API that takes a significant amount of time to return
			String result = service.greeting("worker1:" + 1);
			future.complete(result);
		}, res -> {
			log.info("The result is: " + res.result());
		});
	}

	public void workerExecutor2(Vertx vertx, int i) {
		vertx.executeBlocking(future -> {
			// Call some blocking API that takes a significant amount of time to return
			String result = service.greeting("worker2:" + i);
			future.complete(result);
		}, res -> {
			log.info("The result is: " + res.result());
		});
	}

	public void workerExecutorUni(int i) {
		log.info("@ConsumeEvent. param:" + i);

		Uni.createFrom().item(UUID::randomUUID).emitOn(Infrastructure.getDefaultWorkerPool()).subscribe()
				.with(this::worker, Throwable::printStackTrace);

	}

	private Uni<Void> worker(UUID uuid) {
		MDC.put("work", uuid);
		log.info("Starting work: " + uuid);
		try {
			service.greeting(uuid.toString());
			latch.countDown();
			
		} catch (Exception ex) {
			log.info("Could not finish work: " + uuid);
			throw new RuntimeException(ex);
		}
		log.info("Finish work: {}.", uuid);
		MDC.clear();
		return Uni.createFrom().voidItem();
	}


	@Override
	public int run(String... args) throws Exception {
		 int workersMax=5;
		
		for (int i = 0; i < workersMax; i++) {
			workerExecutorUni(i);
		}
	    latch.await(12, TimeUnit.HOURS);
	    log.info("Finished all threads");
	     
	    Quarkus.asyncExit();
		
		return 0;
	}
	

    void onStart(@Observes StartupEvent ev) {               
        log.info("The application is starting...");
    }

    void onStop(@Observes ShutdownEvent ev) {               
        log.info("The application is stopping...");
    }
	
}