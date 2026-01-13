package com.high.product.infrastructure.redis;

import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

import com.high.product.application.port.SagaDeduplicationPort;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SagaIdDeduplication implements SagaDeduplicationPort {

	private final RedissonClient redissonClient;

	// 이미 존재하는지 체크
	@Override
	public boolean exists(String key) {
		return redissonClient.getBucket(key).isExists(); // [수정] get().get() 대신 isExists()로 효율화
	}

	// 최초 처리 시도
	@Override
	public boolean tryProcess(String key, long ttlSeconds) {
		RBucket<Boolean> bucket = redissonClient.getBucket(key);

		// [수정] 원자적(Atomic) 연산인 setIfAbsent를 사용하여 동시성 환경에서 멱등성 완벽 보장
		return bucket.setIfAbsent(true, java.time.Duration.ofSeconds(ttlSeconds));
	}

	// 최종 실패 상태 등을 강제로 저장할 때 사용
	@Override
	public void save(String key, long ttlSeconds) {
		redissonClient.getBucket(key).set(true, ttlSeconds, TimeUnit.SECONDS);
	}

	// processing key 삭제 (재처리 가능)
	@Override
	public void remove(String key) {
		redissonClient.getBucket(key).delete();
	}
}