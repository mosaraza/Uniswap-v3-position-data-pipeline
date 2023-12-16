WITH decoded_log AS (
  SELECT
    `spock-main.uniswap_v3_positions.decode_pool_creation_log`(data, topics) AS decoded_data
  FROM 
    `spock-main`.`uniswap_v3_positions`.`logs`
  WHERE 
    -- POOL_CREATED
    ARRAY_LENGTH(topics) > 0
    AND
    topics[0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
)

SELECT 
    decoded_data.pool as `pool`,
    decoded_data.token0 as `token_0`,
    decoded_data.token0 as `token_1`,
    decoded_data.fee as `fee`,
    decoded_data.tickSpacing as `tick_spacing`
FROM 
    decoded_log