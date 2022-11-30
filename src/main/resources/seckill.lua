--1、参数列表
--1、优惠券的id
local vouchId = ARGV[1]
--2、用户id
local userId = ARGV[2]
--订单id
local orderId = ARGV[3]

--数据KEY
--1、库存key
local stockKey = 'seckill:stock:'..vouchId
--2、订单key
local orderKey = 'seckill:order:'..vouchId

--脚本业务
--判断库存是否充足
if (tonumber(redis.call('get',stockKey)) <= 0) then
    return 1
end

if (tonumber(redis.call('sismumber',orderKey,userId)) == 1) then
    return 2
end
--扣减库存
redis.call('incrby',stockKey,-1)
--保存用户
redis.call('sadd',orderKey,userId)
--发送消息到队列中
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',vouchId,'id',orderId)
return 0
