
  
    

    create or replace table `spock-main`.`uniswap_v3_positions`.`positions_calculation_data`
    
    

    OPTIONS()
    as (
      WITH decoded_log AS (
  SELECT
    `spock-main.uniswap_v3_positions.decode_swap_log`(data, topics) AS decoded_data,
    LOWER(address) as pool,
    block_number,
    log_index
  FROM 
    `spock-main`.`uniswap_v3_positions`.`filtered_logs`
  WHERE 
    -- SWAP
    topics[OFFSET(0)] = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
)

SELECT 
  pools.pool AS `pool`,
  pools.token_0 AS `token_0`,
  pools.token_1 AS `token_1`,
  pools.decimals_0 AS `decimals_0`,
  pools.decimals_1 AS `decimals_1`,
  pools.fee AS `fee`,
  decoded_data.liquidity AS `liquidity`,
  decoded_data.sqrtPriceX96 AS `sqrt_price_x96`,
  decoded_data.tick AS `tick`
FROM 
  `spock-main`.`uniswap_v3_positions`.`pools` pools
JOIN (
  SELECT 
    decoded_data,
    pool,
    ROW_NUMBER() OVER (PARTITION BY pool ORDER BY block_number DESC, log_index DESC) AS rn
  FROM decoded_log
) latest 
ON
  latest.pool = pools.pool
WHERE latest.rn = 1
    );
  