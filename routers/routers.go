package routers

import (
	"api-caixa/caixa"
	"api-caixa/database/query"
	"api-caixa/logar"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/gin-gonic/gin"
)

// ResponseOK retorna uma mensagem de ok
func ResponseOK(c *gin.Context, log logar.Logfile) {
	c.IndentedJSON(http.StatusOK, "Servidor up")
}

// GetCaixa retorna o ultimo caixa registrado no banco
func GetCaixa(c *gin.Context, log logar.Logfile, dynamoClient *dynamodb.Client) {
	caixa := query.GetLatestCaixa(dynamoClient, log)
	c.IndentedJSON(http.StatusOK, caixa)
}

func Fechar(c *gin.Context, log logar.Logfile, dynamoClient *dynamodb.Client) {
	caixa.Fechar(dynamoClient, log)
	c.IndentedJSON(http.StatusOK, "Caixa Fechado")
}
