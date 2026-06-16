package com.andres.course.springcloud.kafka.command.handlers;

import com.andres.course.springcloud.kafka.command.models.Command;
import com.andres.course.springcloud.kafka.command.models.CommandType;
import com.andres.course.springcloud.kafka.command.models.Reply;
import com.andres.course.springcloud.kafka.command.models.ReplyStatus;
import com.andres.course.springcloud.kafka.command.models.dto.ProductDto;
import com.andres.course.springcloud.kafka.command.services.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class ProductCommandConsumer {
    private static final Logger log = LoggerFactory.getLogger(ProductCommandConsumer.class);

    private final ProductService service;

    public ProductCommandConsumer(ProductService productService) {
        this.service = productService;
    }

    @Bean
    public Function<Message<Command<ProductDto>>, Message<Reply<Object>>> handleCommands() {
        return msg -> {

            String correlationId = msg.getHeaders().get("correlationId", String.class);
            log.info("Recibiendo CorrelationId={}", correlationId);
            if (correlationId == null || correlationId.isBlank()) {
                return MessageBuilder
                        .withPayload(new Reply<>(ReplyStatus.ERROR, "Missing correlationId", null))
                        .build();
            }

            Command<ProductDto> cmd = msg.getPayload();
            Reply<Object> reply = switch (cmd.type()) {
                case CommandType.CREATE -> {
                    if(cmd.body() == null) {
                        log.warn("Create Empty body");
                        yield new Reply<>(ReplyStatus.ERROR, "Create Empty body", null);
                    } else {
                        ProductDto productSave = service.create(cmd.body());

                        log.info("Creating product name={}, price={}", productSave.name(), productSave.price());
                        yield new Reply<>(ReplyStatus.SUCCESS, "Create product name", productSave);
                    }
                }
                case CommandType.READ -> {
                    if(cmd.id() == null) {
                        log.warn("Id is required");
                        yield new Reply<>(ReplyStatus.ERROR, "Id is required", null);
                    } else {
                        ProductDto dto = service.findById(cmd.id());
                        log.info("Reading product by id");
                        yield (dto == null) ?
                                new Reply<>(ReplyStatus.ERROR, "Product not found", null) :
                                new Reply<>(ReplyStatus.SUCCESS, "Read product name", dto);
                    }
                }
                case CommandType.READ_ALL -> {
                    log.info("Reading all products");
                    yield new Reply<>(ReplyStatus.SUCCESS, "Read all products", service.findAll());
                }
                case CommandType.UPDATE -> {
                    if(cmd.body() == null || cmd.id() == null) {
                        log.warn("Id and body is required");
                        yield new Reply<>(ReplyStatus.ERROR, "Id and body is required", null);
                    } else {
                        ProductDto dto = service.update(cmd.id(), cmd.body());

                        if (dto != null) {
                            log.info("Updating product name={}, price={}", dto.name(), dto.price());
                            yield new Reply<>(ReplyStatus.SUCCESS, "Update product name", dto);
                        } else {
                            log.info("Product not found, null dto");
                            yield new Reply<>(ReplyStatus.ERROR, "Product not found", null);
                        }
                    }
                }
                case CommandType.DELETE -> {
                    if(cmd.id() == null) {
                        log.warn("Id is required");
                        yield new Reply<>(ReplyStatus.ERROR, "Id is required", null);
                    } else {
                        boolean result = service.delete(cmd.id());
                        log.info("Deleting product");
                        yield (result) ? new Reply<>(ReplyStatus.SUCCESS, "Deleting Product", "deleted") :
                                new Reply<>(ReplyStatus.ERROR, "Product not found", null);
                    }

                }
                default -> {
                    log.warn("Unknown command type={}", cmd.type());
                    yield new Reply<>(ReplyStatus.ERROR, "Unknown command type", null);
                }
            };

            return MessageBuilder.withPayload(reply)
                    .setHeader("correlationId", correlationId)
                    .build();
        };
    }
}
