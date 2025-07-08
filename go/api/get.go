package api

import (
	"encoding/hex"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/keep-pwr-strong/pwr-stateful-vida/database"
)

// GET provides HTTP GET endpoint handlers
type GET struct{}

// NewGET creates a new GET handler instance
func NewGET() *GET {
	return &GET{}
}

// RegisterRoutes initializes and registers all GET endpoint handlers with the Gin framework.
// Currently registers the /rootHash endpoint for retrieving Merkle root hashes
// for specific block numbers.
func (g *GET) RegisterRoutes(router *gin.Engine) {
	router.GET("/rootHash", g.handleRootHash)
}

// RegisterRoutes registers all GET routes (static method for convenience)
func RegisterRoutes(router *gin.Engine) {
	g := NewGET()
	g.RegisterRoutes(router)
}

// handleRootHash handles GET /rootHash requests
// Query Parameters:
//   - blockNumber: The block number to get the root hash for
//
// Returns:
//   - 200: Hex-encoded root hash string
//   - 400: Error message for invalid block number or missing hash
//   - 500: Internal server error
func (g *GET) handleRootHash(c *gin.Context) {
	// Get blockNumber query parameter
	blockNumberStr := c.Query("blockNumber")
	if blockNumberStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Missing blockNumber query parameter",
		})
		return
	}

	// Parse block number
	blockNumber, err := strconv.ParseInt(blockNumberStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid blockNumber format",
		})
		return
	}

	// Get last checked block
	lastCheckedBlock, err := database.GetLastCheckedBlock()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to get last checked block",
		})
		return
	}

	// Handle different block number scenarios
	if blockNumber == lastCheckedBlock {
		// Return current root hash
		rootHash, err := database.GetRootHash()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to get current root hash",
			})
			return
		}
		
		if rootHash == nil {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "Root hash not available",
			})
			return
		}

		c.String(http.StatusOK, hex.EncodeToString(rootHash))
		return

	} else if blockNumber < lastCheckedBlock && blockNumber > 1 {
		// Return historical block root hash
		blockRootHash, err := database.GetBlockRootHash(blockNumber)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to get block root hash",
			})
			return
		}

		if blockRootHash != nil {
			c.String(http.StatusOK, hex.EncodeToString(blockRootHash))
			return
		} else {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Block root hash not found for block number: " + blockNumberStr,
			})
			return
		}

	} else {
		// Invalid block number
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid block number",
		})
		return
	}
}

// Alternative implementation using standard net/http (if you prefer not to use Gin)

// RegisterHTTPRoutes registers routes using standard net/http mux
func RegisterHTTPRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/rootHash", handleRootHashHTTP)
}

// handleRootHashHTTP handles GET /rootHash requests using standard net/http
func handleRootHashHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get blockNumber query parameter
	blockNumberStr := r.URL.Query().Get("blockNumber")
	if blockNumberStr == "" {
		http.Error(w, "Missing blockNumber query parameter", http.StatusBadRequest)
		return
	}

	// Parse block number
	blockNumber, err := strconv.ParseInt(blockNumberStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid blockNumber format", http.StatusBadRequest)
		return
	}

	// Get last checked block
	lastCheckedBlock, err := database.GetLastCheckedBlock()
	if err != nil {
		http.Error(w, "Failed to get last checked block", http.StatusInternalServerError)
		return
	}

	// Handle different block number scenarios
	if blockNumber == lastCheckedBlock {
		// Return current root hash
		rootHash, err := database.GetRootHash()
		if err != nil {
			http.Error(w, "Failed to get current root hash", http.StatusInternalServerError)
			return
		}
		
		if rootHash == nil {
			http.Error(w, "Root hash not available", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(hex.EncodeToString(rootHash)))
		return

	} else if blockNumber < lastCheckedBlock && blockNumber > 1 {
		// Return historical block root hash
		blockRootHash, err := database.GetBlockRootHash(blockNumber)
		if err != nil {
			http.Error(w, "Failed to get block root hash", http.StatusInternalServerError)
			return
		}

		if blockRootHash != nil {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(hex.EncodeToString(blockRootHash)))
			return
		} else {
			http.Error(w, "Block root hash not found for block number: "+blockNumberStr, http.StatusBadRequest)
			return
		}

	} else {
		// Invalid block number
		http.Error(w, "Invalid block number", http.StatusBadRequest)
		return
	}
}
