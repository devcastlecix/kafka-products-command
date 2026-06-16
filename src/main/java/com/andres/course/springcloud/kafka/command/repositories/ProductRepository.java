package com.andres.course.springcloud.kafka.command.repositories;


import com.andres.course.springcloud.kafka.command.entities.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {
}
