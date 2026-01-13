package com.high.product.infrastructure.redis;

import java.util.UUID;

import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.high.product.application.port.RedisCacheEvictPort;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class RedisCacheEvictAdapter implements RedisCacheEvictPort {

	private static final String STOCK_CACHE = "stock";

	private final CacheManager cacheManager;

	@Override
	public void evictStockCacheAfterCommit(UUID productId) {

		// [수정] Serializer와의 정합성을 위해 UUID를 String으로 변환
		String cacheKey = productId.toString();

		if (!TransactionSynchronizationManager.isSynchronizationActive()) {
			evict(cacheKey);
			return;
		}

		TransactionSynchronizationManager.registerSynchronization(
			new TransactionSynchronization() {
				@Override
				public void afterCommit() {
					evict(cacheKey);
				}
			}
		);
	}

	// [수정] 파라미터 타입을 String으로 변경하여 일관성 유지
	private void evict(String cacheKey) {
		if (cacheManager.getCache(STOCK_CACHE) != null) {
			cacheManager.getCache(STOCK_CACHE).evict(cacheKey);
		}
	}
}