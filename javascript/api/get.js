import DatabaseService from '../databaseService.js';

export class GET {
    static run(app) {
        app.get('/rootHash', async (req, res) => {
            try {
                const response = await this.handleRootHash(req.query);
                
                if (response === '') {
                    return res.status(500).send('');
                }
                
                if (response.startsWith('Block root hash not found') || 
                    response === 'Invalid block number') {
                    return res.status(400).send(response);
                }
                
                return res.send(response);
                
            } catch (error) {
                return res.status(500).send('');
            }
        });
    }

    static async handleRootHash(params) {
        const blockNumberStr = params.blockNumber;
        if (!blockNumberStr) {
            throw new Error('Missing blockNumber parameter');
        }
        
        const blockNumber = parseInt(blockNumberStr, 10);
        if (isNaN(blockNumber)) {
            throw new Error('Invalid block number format');
        }
        
        const lastCheckedBlock = await DatabaseService.getLastCheckedBlock();
        
        if (blockNumber === lastCheckedBlock) {
            const rootHash = await DatabaseService.getRootHash();
            if (rootHash) {
                return rootHash.toString('hex');
            } else {
                return '';
            }
        }
        else if (blockNumber < lastCheckedBlock && blockNumber > 1) {
            const blockRootHash = await DatabaseService.getBlockRootHash(blockNumber);
            
            if (blockRootHash !== null) {
                return blockRootHash.toString('hex');
            } else {
                return `Block root hash not found for block number: ${blockNumber}`;
            }
        } else {
            return 'Invalid block number';
        }
    }
}

export default { GET };