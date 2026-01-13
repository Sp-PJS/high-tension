package com.high.order.infrastructure.client;

import com.high.order.application.dto.external.ProductResponse;
import com.high.order.application.service.ProductService;
import com.high.order.infrastructure.config.feign.FeignConfig;
import com.library.module.response.ApiResponse;

import java.util.UUID;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "product-service", configuration = FeignConfig.class)
public interface ProductClient extends ProductService {
	// [수정] 내부 통신 API로 경로 변경(@Cacheable이 붙지 않은 서비스 메서드 호출)
	@GetMapping("/api/v1/internal/products/{productId}")
	ApiResponse<ProductResponse> getProductByOrderId(
		@PathVariable("productId") UUID productId);

}
