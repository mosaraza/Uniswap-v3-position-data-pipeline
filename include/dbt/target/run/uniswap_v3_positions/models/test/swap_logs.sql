
  
    

    create or replace table `spock-main`.`uniswap_v3_positions`.`swap_logs`
    
    

    OPTIONS()
    as (
      WITH decoded_log AS (
  SELECT
    `spock-main.uniswap_v3_positions.decode_swap_log`(data, topics) AS decoded_data, address as `pool`
  FROM 
    `spock-main`.`uniswap_v3_positions`.`pool_logs` 
  WHERE 
    -- MINT
    topics[0] = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
)

SELECT 
    decoded_data.sender AS `sender`,
    decoded_data.recipient AS `recipient`,
    decoded_data.amount0 AS `amount_0`,
    decoded_data.amount1 AS `amount_1`,
    decoded_data.liquidity AS `liquidity`,
    decoded_data.sqrtPriceX96 AS `sqrt_price_x96`,
    decoded_data.tick AS `tick`,
    `pool`
FROM 
    decoded_log
    );
  