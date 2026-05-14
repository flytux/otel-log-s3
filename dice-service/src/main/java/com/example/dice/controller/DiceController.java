package com.example.dice.controller;

import com.example.dice.service.LogGeneratorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ThreadLocalRandom;

@RestController
public class DiceController {

    private static final Logger logger = LoggerFactory.getLogger(DiceController.class);

    private final LogGeneratorService logGeneratorService;

    public DiceController(LogGeneratorService logGeneratorService) {
        this.logGeneratorService = logGeneratorService;
    }

    @GetMapping("/rolldice")
    public String rolldice(@RequestParam(required = false) String player) {
        int result = ThreadLocalRandom.current().nextInt(1, 7);
        String userId = (player != null) ? player : "anonymous";

        MDC.put("endpoint", "/rolldice");
        MDC.put("http_method", "GET");
        MDC.put("user_id", userId);
        try {
            if (player == null) {
                logger.info("Anonymous player is rolling the dice: {}", result);
            } else {
                logger.info("Player {} is rolling the dice: {}", player, result);
            }
            // Generate additional structured logs per request
            logGeneratorService.generateRandomLogs(player, result);
        } finally {
            MDC.clear();
        }

        return String.valueOf(result);
    }
}
