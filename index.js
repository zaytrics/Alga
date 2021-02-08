const pools = require("./db");
var pool2= '';
const axios = require('axios');

exports.handler = async(event, context, callback) => {
    let message =  await keepingLiveRecord();
    context.callbackWaitsForEmtpyEventLoop = false;
    const response = {
        statusCode: 200,
        body: JSON.stringify(message),
    };
    console.log("response",response)
    return response;

};
const keepingLiveRecord = async() =>{
    try {
        pool2= await pools.getPool();
        //Getting Total number of datasets from market Oceans 
        const response = await axios.get('https://market-stats.oceanprotocol.com/')
        totalDatasets = response.data.datasets.total;
        console.log("Total data sets in market place",totalDatasets); 
        const offset=totalDatasets;
        // Updating price and other details of the assets
        const query={
            "page": 1,
            "offset": offset,
            "query": {
                "nativeSearch": 1,
                "query_string": {
                    "query": "-isInPurgatory:true"
                }
            },
            "sort": {
                "created": -1
            }
        }    
        console.log("Getting All datasets from market...");
        const response2 = await axios.post('https://aquarius.mainnet.oceanprotocol.com/api/v1/aquarius/assets/ddo/query',query);
        const result = response2.data.results
        var i;
            for(i=0;i<totalDatasets;i++)
            {
                // Updating new dataset to the database
                const one = await updateDataSetToDb(result[i]);  
                console.log("loop itter: ",i);
            }
        console.log("update done")            
    } catch (error) {
        console.log(error.message)
    }  
    console.log("Reached near callback")  
    // callback(null, 'Hello from Lambda');
    console.log("after callback")  
    let msg = 'done';
    return msg;
}
const updateDataSetToDb = async(data) =>{
        //getting Asset info
        const {id,service,publicKey,dataTokenInfo,price} = data;
        const Author_address = publicKey[0].owner

        const {main,additionalInformation}=service[0].attributes;
        const {description,tags,links} = additionalInformation;

        const Data_sample_url=typeof links == "undefined"? 0:typeof links[0] == 'undefined'?0:links[0].url;
        
        const {name,dateCreated,author,datePublished} =main;
        const {address,symbol,decimals,totalSupply,cap,minter,minterBalance} =dataTokenInfo;
        const Data_token_name=dataTokenInfo.name;
        const {datatoken,value,type} =price;
        
        //Update datatoken to db
        
        
            const res = await pool2.query(
                "UPDATE Asset SET Data_token_decimals = '"+decimals+"',Data_token_supply = '"+totalSupply+"',Dataz_token_cap= '"+cap+"',Minter_balance='"+minterBalance+"',Data_token_amount='"+datatoken+"',Data_token_value ='"+value+"' WHERE Asset_id = '"+id+"'"   
            );
        //Add to asset history
        const nowsTime = Date.now();
        const res2 = await pool2.query(
            "INSERT INTO Asset_price_history(Asset_id,Data_token_name,Data_token_amount,Data_token_value,Dataset_name) VALUES($1,$2,$3,$4,$5)",
            [id,Data_token_name,datatoken,value,name]
        );
        //getting pool info if asset has pool
        console.log("Pool type:",type);
        let pool_address=price.address;
        pool_address=pool_address.toLowerCase()
        console.log(pool_address)
        const query2 = {"operationName":"PoolLiquidity","variables":{"id":pool_address,"shareId":""+pool_address+"-0x655eFe6Eb2021b8CEfE22794d90293aeC37bb325"},"query":"query PoolLiquidity($id: ID!, $shareId: ID) {\n  pool(id: $id) {\n    id\n    totalShares\n    swapFee\n    tokens {\n      tokenAddress\n      balance\n      denormWeight\n      __typename\n    }\n    shares(where: {id: $shareId}) {\n      id\n      balance\n      __typename\n    }\n    __typename\n  }\n}\n"}
        if(type=='pool'){
            const response2 = await axios.post('https://subgraph.mainnet.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph',query2);
            poolinfo=response2.data.data.pool;
            if(poolinfo.tokens[0].tokenAddress=='0x967da4048cd07ab37855c090aaf366e4ce1b9f48')
            {
                ocean=poolinfo.tokens[0]
                token=poolinfo.tokens[1]
            }else{
                ocean=poolinfo.tokens[1]
                token=poolinfo.tokens[0]
            }
            //Updating pool to db.
            console.log("Updating pool")
            const res3 = await pool2.query(
                "UPDATE pool SET token_balance='"+token.balance+"',ocean_balance='"+ocean.balance+"',pool_share='"+poolinfo.totalShares+"',swap_fee='"+poolinfo.swapFee+"',denorm_weight ='"+token.denormWeight+"' WHERE pool_id = '"+pool_address+"'",
            );
            //Adding pool history
            const res4 = await pool2.query(
                "INSERT INTO Pool_price_history(Pool_id,Data_token_name,Token_balance,Ocean_balance,Dataset_name) VALUES($1,$2,$3,$4,$5)",
                [pool_address,Data_token_name,token.balance,ocean.balance,name]
            );
        }else{
            //Do nothing
        }
        return 1;
}


