package com.andres.course.springcloud.kafka.command.models.mapper;

import com.andres.course.springcloud.kafka.command.entities.Product;
import com.andres.course.springcloud.kafka.command.models.dto.ProductDto;

public final class Mappers {

    private Mappers() {}

    static public ProductDto toDto(Product product) {
        return new ProductDto(product.getId(), product.getName(), product.getPrice());
    }

    static public Product toEntity(ProductDto dto) {
        Product entity =  new Product(dto.name(), dto.price());
        entity.setId(dto.id());
        return entity;
    }
}
