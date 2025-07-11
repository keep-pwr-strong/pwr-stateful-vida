package api

import (
	"encoding/hex"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/keep-pwr-strong/pwr-stateful-vida/database"
)

func RegisterRoutes(router *gin.Engine) {
	router.GET("/rootHash", func(c *gin.Context) {
		blockNumber, _ := strconv.ParseInt(c.Query("blockNumber"), 10, 64)
		lastCheckedBlock, _ := database.GetLastCheckedBlock()

		if blockNumber == lastCheckedBlock {
			if rootHash, _ := database.GetRootHash(); rootHash != nil {
				c.String(http.StatusOK, hex.EncodeToString(rootHash))
				return
			}
		} else if blockNumber < lastCheckedBlock && blockNumber > 1 {
			if blockRootHash, _ := database.GetBlockRootHash(blockNumber); blockRootHash != nil {
				c.String(http.StatusOK, hex.EncodeToString(blockRootHash))
				return
			}
			c.String(http.StatusBadRequest, "Block root hash not found for block number: "+c.Query("blockNumber"))
			return
		}

		c.String(http.StatusBadRequest, "Invalid block number")
	})
}
