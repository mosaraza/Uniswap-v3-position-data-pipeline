
  
    

    create or replace table `spock-main`.`uniswap_v3_positions`.`pools`
    
    

    OPTIONS()
    as (
      WITH decoded_log AS (
  SELECT
    `spock-main.uniswap_v3_positions.decode_pool_creation_log`(data, topics) AS decoded_data
  FROM 
    `spock-main`.`uniswap_v3_positions`.`filtered_logs`
  WHERE
    -- POOL_CREATED
    topics[0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
)

SELECT 
    LOWER(decoded_data.pool) as `pool`,
    LOWER(decoded_data.token0) as `token_0`,
    LOWER(decoded_data.token1) as `token_1`,
    tokens_0.decimals as `decimals_0`,
    tokens_1.decimals as `decimals_1`,
    decoded_data.fee as `fee`,
    decoded_data.tickSpacing as `tick_spacing`,
FROM 
    decoded_log
LEFT JOIN 
    `spock-main`.`uniswap_v3_positions`.`tokens` AS tokens_0 ON LOWER(decoded_log.decoded_data.token0) = LOWER(tokens_0.address)
LEFT JOIN 
    `spock-main`.`uniswap_v3_positions`.`tokens` AS tokens_1 ON LOWER(decoded_log.decoded_data.token1) = LOWER(tokens_1.address)
    );
  