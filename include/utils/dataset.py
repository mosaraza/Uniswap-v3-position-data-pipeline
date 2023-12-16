from include.utils.connections import gcp_conn_id

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

project = 'spock-main'
dataset_id = 'uniswap_v3_positions'

def add_dataset():
    return BigQueryCreateEmptyDatasetOperator(
        task_id = 'add_dataset',
        dataset_id = dataset_id,
        gcp_conn_id = gcp_conn_id,
        if_exists = 'skip',
        trigger_rule='all_done',
    )

def add_reference_tables():
    query = f"""
        CREATE OR REPLACE VIEW `{dataset_id}.blocks` AS
        SELECT *
        FROM `bigquery-public-data.crypto_ethereum.blocks`;

        CREATE OR REPLACE VIEW `{dataset_id}.transactions` AS
        SELECT *
        FROM `bigquery-public-data.crypto_ethereum.transactions`;

        CREATE OR REPLACE VIEW `{dataset_id}.logs` AS
        SELECT *
        FROM `bigquery-public-data.crypto_ethereum.logs`;

        CREATE OR REPLACE VIEW `{dataset_id}.traces` AS
        SELECT *
        FROM `bigquery-public-data.crypto_ethereum.traces`;
        
        CREATE OR REPLACE VIEW `{dataset_id}.tokens` AS
        SELECT *
        FROM `bigquery-public-data.crypto_ethereum.tokens`;

        /* Function to decode uniswap v3 pool's mint log */
        CREATE OR REPLACE FUNCTION `{project}.{dataset_id}.decode_mint_log`(log_data STRING, topics ARRAY<STRING>) RETURNS STRUCT<sender STRING, owner STRING, tickLower NUMERIC, tickUpper NUMERIC, amount FLOAT64, amount0 FLOAT64, amount1 FLOAT64> LANGUAGE js
        OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS '''
        var abi = [
            {{
                anonymous: false,
                inputs: [
                    {{
                        "indexed": false,
                        "internalType": "address",
                        "name": "sender",
                        "type": "address",
                    }},
                    {{
                        "indexed": true,
                        "internalType": "address",
                        "name": "owner",
                        "type": "address",
                    }},
                    {{
                        "indexed": true,
                        "internalType": "int24",
                        "name": "tickLower",
                        "type": "int24",
                    }},
                    {{
                        "indexed": true,
                        "internalType": "int24",
                        "name": "tickUpper",
                        "type": "int24",
                    }},
                    {{
                        "indexed": false,
                        "internalType": "uint128",
                        "name": "amount",
                        "type": "uint128",
                    }},
                    {{
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "amount0",
                        "type": "uint256",
                    }},
                    {{
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "amount1",
                        "type": "uint256",
                    }},
                ],
                name: "Mint",
                type: "event",
            }}
        ];

        var interface_instance = new ethers.utils.Interface(abi);

        try {{
            var parsedLog = interface_instance.parseLog({{topics: topics, data: log_data}});
            return {{
                sender: parsedLog.values.sender,
                owner: parsedLog.values.owner,
                tickLower: Number(parsedLog.values.tickLower),
                tickUpper: Number(parsedLog.values.tickUpper),
                amount: Number(parsedLog.values.amount),
                amount0: Number(parsedLog.values.amount0),
                amount1: Number(parsedLog.values.amount1),
            }};
        }} catch (e) {{
            return null;
        }}
        ''';
        
        
        /* Function to decode uniswap v3 pool's burn log */
        CREATE OR REPLACE FUNCTION `{project}.{dataset_id}.decode_burn_log`(log_data STRING, topics ARRAY<STRING>) RETURNS STRUCT<sender STRING, owner STRING, tickLower NUMERIC, tickUpper NUMERIC, amount FLOAT64, amount0 FLOAT64, amount1 FLOAT64> LANGUAGE js
        OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS '''
        var abi = [
            {{
                anonymous: false,
                inputs: [
                    {{
                        "indexed": true,
                        "internalType": "address",
                        "name": "owner",
                        "type": "address",
                    }},
                    {{
                        "indexed": true,
                        "internalType": "int24",
                        "name": "tickLower",
                        "type": "int24",
                    }},
                    {{
                        "indexed": true,
                        "internalType": "int24",
                        "name": "tickUpper",
                        "type": "int24",
                    }},
                    {{
                        "indexed": false,
                        "internalType": "uint128",
                        "name": "amount",
                        "type": "uint128",
                    }},
                    {{
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "amount0",
                        "type": "uint256",
                    }},
                    {{
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "amount1",
                        "type": "uint256",
                    }},
                ],
                name: "Burn",
                type: "event",
            }}
        ];

        var interface_instance = new ethers.utils.Interface(abi);

        try {{
            var parsedLog = interface_instance.parseLog({{topics: topics, data: log_data}});
            return {{
                sender: parsedLog.values.sender,
                owner: parsedLog.values.owner,
                tickLower: Number(parsedLog.values.tickLower),
                tickUpper: Number(parsedLog.values.tickUpper),
                amount: Number(parsedLog.values.amount),
                amount0: Number(parsedLog.values.amount0),
                amount1: Number(parsedLog.values.amount1),
            }};
        }} catch (e) {{
            return null;
        }}
        ''';
        
        
        /* Function to decode uniswap v3 pool's swap log */
        CREATE OR REPLACE FUNCTION `{project}.{dataset_id}.decode_swap_log`(log_data STRING, topics ARRAY<STRING>) RETURNS STRUCT<sender STRING, recipient STRING, amount0 FLOAT64, amount1 FLOAT64, sqrtPriceX96 FLOAT64, liquidity FLOAT64, tick FLOAT64> LANGUAGE js
        OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS '''
        var abi = [
            {{
                "anonymous": false,
                "inputs": [
                {{
                    "indexed": true,
                    "internalType": "address",
                    "name": "sender",
                    "type": "address"
                }},
                {{
                    "indexed": true,
                    "internalType": "address",
                    "name": "recipient",
                    "type": "address"
                }},
                {{
                    "indexed": false,
                    "internalType": "int256",
                    "name": "amount0",
                    "type": "int256"
                }},
                {{
                    "indexed": false,
                    "internalType": "int256",
                    "name": "amount1",
                    "type": "int256"
                }},
                {{
                    "indexed": false,
                    "internalType": "uint160",
                    "name": "sqrtPriceX96",
                    "type": "uint160"
                }},
                {{
                    "indexed": false,
                    "internalType": "uint128",
                    "name": "liquidity",
                    "type": "uint128"
                }},
                {{
                    "indexed": false,
                    "internalType": "int24",
                    "name": "tick",
                    "type": "int24"
                }}
                ],
                "name": "Swap",
                "type": "event"
            }}
        ];

        var interface_instance = new ethers.utils.Interface(abi);

        try {{
            var parsedLog = interface_instance.parseLog({{topics: topics, data: log_data}});
            return {{
                sender: parsedLog.values.sender,
                recipient: parsedLog.values.recipient, 
                amount0: Number(parsedLog.values.amount0), 
                amount1: Number(parsedLog.values.amount1), 
                sqrtPriceX96: Number(parsedLog.values.sqrtPriceX96),
                liquidity: Number(parsedLog.values.liquidity),
                tick: Number(parsedLog.values.tick)
            }};
        }} catch (e) {{
            return null;
        }}
        ''';
        
        
        /* Function to decode uniswap v3 pool creation log */
        CREATE OR REPLACE FUNCTION `{project}.{dataset_id}.decode_pool_creation_log`(log_data STRING, topics ARRAY<STRING>) RETURNS STRUCT<token0 STRING, token1 STRING, fee NUMERIC, tickSpacing NUMERIC, pool STRING> LANGUAGE js
        OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS '''
        var abi = [
            {{
                "anonymous": false,
                "inputs": [
                {{
                    "indexed": true,
                    "internalType": "address",
                    "name": "token0",
                    "type": "address"
                }},
                {{
                    "indexed": true,
                    "internalType": "address",
                    "name": "token1",
                    "type": "address"
                }},
                {{
                    "indexed": true,
                    "internalType": "uint24",
                    "name": "fee",
                    "type": "uint24"
                }},
                {{
                    "indexed": false,
                    "internalType": "int24",
                    "name": "tickSpacing",
                    "type": "int24"
                }},
                {{
                    "indexed": false,
                    "internalType": "address",
                    "name": "pool",
                    "type": "address"
                }}
                ],
                "name": "PoolCreated",
                "type": "event"
            }}
        ];

        var interface_instance = new ethers.utils.Interface(abi);

        try {{
            var parsedLog = interface_instance.parseLog({{topics: topics, data: log_data}});
            return {{
                token0: parsedLog.values.token0,
                token1: parsedLog.values.token1,
                pool: parsedLog.values.pool,
                fee: Number(parsedLog.values.fee),
                tickSpacing: Number(parsedLog.values.tickSpacing)
            }};
        }} catch (e) {{
            return null;
        }}
        ''';
        
        
        /* Function to decode uniswap v3 position manager's increase liquidity log */
        CREATE OR REPLACE FUNCTION `{project}.{dataset_id}.decode_increase_liquidity_log`(log_data STRING, topics ARRAY<STRING>) RETURNS STRUCT<tokenId NUMERIC, liquidity FLOAT64> LANGUAGE js
        OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS '''
        var abi = [
            {{
                "anonymous": false,
                "inputs": [
                {{
                    "indexed": true,
                    "internalType": "uint256",
                    "name": "tokenId",
                    "type": "uint256"
                }},
                {{
                    "indexed": false,
                    "internalType": "uint128",
                    "name": "liquidity",
                    "type": "uint128"
                }},
                {{
                    "indexed": false,
                    "internalType": "uint256",
                    "name": "amount0",
                    "type": "uint256"
                }},
                {{
                    "indexed": false,
                    "internalType": "uint256",
                    "name": "amount1",
                    "type": "uint256"
                }}
                ],
                "name": "IncreaseLiquidity",
                "type": "event"
            }}
        ];

        var interface_instance = new ethers.utils.Interface(abi);

        try {{
            var parsedLog = interface_instance.parseLog({{topics: topics, data: log_data}});
            return {{
                tokenId: Number(parsedLog.values.tokenId),
                liquidity: Number(parsedLog.values.liquidity)
            }};
        }} catch (e) {{
            return null;
        }}
        ''';
        
        
        
        /* Function to decode uniswap v3 position manager's decrease liquidity log */
        CREATE OR REPLACE FUNCTION `{project}.{dataset_id}.decode_decrease_liquidity_log`(log_data STRING, topics ARRAY<STRING>) RETURNS STRUCT<tokenId NUMERIC, liquidity FLOAT64> LANGUAGE js
        OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS '''
        var abi = [
            {{
                "anonymous": false,
                "inputs": [
                {{
                    "indexed": true,
                    "internalType": "uint256",
                    "name": "tokenId",
                    "type": "uint256"
                }},
                {{
                    "indexed": false,
                    "internalType": "uint128",
                    "name": "liquidity",
                    "type": "uint128"
                }},
                {{
                    "indexed": false,
                    "internalType": "uint256",
                    "name": "amount0",
                    "type": "uint256"
                }},
                {{
                    "indexed": false,
                    "internalType": "uint256",
                    "name": "amount1",
                    "type": "uint256"
                }}
                ],
                "name": "DecreaseLiquidity",
                "type": "event"
            }}
        ];

        var interface_instance = new ethers.utils.Interface(abi);

        try {{
            var parsedLog = interface_instance.parseLog({{topics: topics, data: log_data}});
            return {{
                tokenId: Number(parsedLog.values.tokenId),
                liquidity: Number(parsedLog.values.liquidity)
            }};
        }} catch (e) {{
            return null;
        }}
        ''';
        
        
        /* Function to decode uniswap v3 positions */
        CREATE OR REPLACE FUNCTION `spock-main.uniswap_v3_positions.decode_position`(
            chain INT64,
            token0 STRING,
            token1 STRING,
            decimal0 STRING,
            decimal1 STRING,
            feeTier NUMERIC,
            sqrtPrice FLOAT64,
            poolLiquidity FLOAT64,
            tick FLOAT64,
            walletLiquidity FLOAT64,
            tickLower NUMERIC,
            tickUpper NUMERIC
        ) RETURNS STRUCT<amount_0 FLOAT64, amount_1 FLOAT64> LANGUAGE js
        OPTIONS (library=["gs://libraries-bigquery/uniswap-v3-position.js"]) AS '''
        try {{
            var {{amount0,amount1}} = v3Position.decodePosition(
                Number(chain),
                token0,
                token1,
                Number(decimal0),
                Number(decimal1),
                Number(feeTier),
                Number(sqrtPrice),
                Number(poolLiquidity),
                Number(tick),
                Number(walletLiquidity),
                Number(tickLower),
                Number(tickUpper)
            );

            return {{
                amount_0: amount0,
                amount_1: amount1
            }};
        }} catch (e) {{
            return null;
        }}
    ''';
    """

    return BigQueryExecuteQueryOperator(
        task_id ='add_reference_tables',
        sql = query,
        use_legacy_sql = False,
        gcp_conn_id = 'gcp',
        trigger_rule='all_done',  
    )