package api

import (
	"encoding/hex"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"pwr-stateful-vida/dbservice"
)

func RegisterRoutes(router *gin.Engine) {
	router.GET("/rootHash", func(c *gin.Context) {
		blockNumber, _ := strconv.ParseInt(c.Query("blockNumber"), 10, 64)
		lastCheckedBlock, _ := dbservice.GetLastCheckedBlock()

		if blockNumber == lastCheckedBlock {
			if rootHash, _ := dbservice.GetRootHash(); rootHash != nil {
				c.String(http.StatusOK, hex.EncodeToString(rootHash))
				return
			}
		} else if blockNumber < lastCheckedBlock && blockNumber > 1 {
			if blockRootHash, _ := dbservice.GetBlockRootHash(blockNumber); blockRootHash != nil {
				c.String(http.StatusOK, hex.EncodeToString(blockRootHash))
				return
			}
			c.String(http.StatusBadRequest, "Block root hash not found for block number: "+c.Query("blockNumber"))
			return
		}

		c.String(http.StatusBadRequest, "Invalid block number")
	})
}
