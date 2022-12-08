package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testId() throws InterruptedException {
        System.out.println(1);
        System.out.println(2);
        System.out.println(3);
        System.out.println("master test");
        System.out.println("hot-fix test");
        System.out.println("pull test");
        System.out.println("gitee test");
        System.out.println("gitee pull test");
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () ->{
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };
        long beginTime = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long endTime = System.currentTimeMillis();
        System.out.println("time = " +(endTime - beginTime));
    }

    @Test
    void testShop() throws InterruptedException {
        long id = redisIdWorker.nextId("pro:id:");
        System.out.println(id);
    }

    @Test
    void load(){
        //查询店铺的信息
        List<Shop> shopList = shopService.list();
        //把店铺分组 通过typeid 一致的分到一个集合中
        Map<Long, List<Shop>> longListMap = shopList.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //分批完成写入redis
        for (Map.Entry<Long, List<Shop>> entry : longListMap.entrySet()) {
            Long typeId = entry.getKey();
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            //写入redis
            for (Shop shop : value) {
                //stringRedisTemplate.opsForGeo()
                       // .add(SHOP_GEO_KEY+typeId,new Point(shop.getX(),shop.getY()),shop.getId().toString());
               locations.add(new RedisGeoCommands.GeoLocation<>(
                       shop.getId().toString(),new Point(shop.getX(),shop.getY())
               ));
            }
            stringRedisTemplate.opsForGeo()
                    .add(SHOP_GEO_KEY+typeId,locations);
        }
    }

}
