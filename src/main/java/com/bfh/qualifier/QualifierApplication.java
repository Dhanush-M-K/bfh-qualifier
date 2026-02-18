package com.bfh.qualifier;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.client.WebClient;


@SpringBootApplication
public class QualifierApplication {

    public static void main(String[] args) {
        SpringApplication.run(QualifierApplication.class, args);
    }

    @Bean
    CommandLineRunner run() {
        return args -> {

            WebClient webClient = WebClient.create();

            WebhookResponse response = webClient.post()
                    .uri("https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA")
                    .bodyValue(java.util.Map.of(
                            "name", "Dhanush M K",
                            "regNo", "REG21047",
                            "email", "dhanushmk0001@gmail.com"
                    ))
                    .retrieve()
                    .bodyToMono(WebhookResponse.class)
                    .block();

            System.out.println("Webhook URL: " + response.getWebhook());
            System.out.println("Access Token: " + response.getAccessToken());
            String finalQuery = """
            		SELECT department_name, salary, employee_name, age
            		FROM (
            		    SELECT 
            		        d.department_name,
            		        SUM(p.amount) AS salary,
            		        e.first_name || ' ' || e.last_name AS employee_name,
            		        EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM e.dob) AS age,
            		        ROW_NUMBER() OVER (
            		            PARTITION BY d.department_id
            		            ORDER BY SUM(p.amount) DESC
            		        ) AS rn
            		    FROM employee e
            		    JOIN department d ON e.department = d.department_id
            		    JOIN payments p ON e.emp_id = p.emp_id
            		    WHERE EXTRACT(DAY FROM p.payment_time) <> 1
            		    GROUP BY e.emp_id, d.department_id, d.department_name, e.first_name, e.last_name, e.dob
            		) ranked
            		WHERE rn = 1
            		""";
            String submissionResponse = webClient.post()
                    .uri(response.getWebhook())
                    .header("Authorization", response.getAccessToken())
                    .bodyValue(java.util.Map.of(
                            "finalQuery", finalQuery
                    ))
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            System.out.println("Submission Response: " + submissionResponse);
        };
    }
}