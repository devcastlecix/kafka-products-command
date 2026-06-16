package com.andres.course.springcloud.kafka.command.services;

import com.andres.course.springcloud.kafka.command.models.dto.ProductDto;

import java.util.List;

public interface ProductService {
    List<ProductDto> findAll();
    ProductDto findById(Long id);
    ProductDto create(ProductDto dto);
    ProductDto update(Long id, ProductDto dto);
    boolean delete(Long id);
}
