package caixa

import (
	"api-caixa/database/query"
	"api-caixa/logar"
	"api-caixa/model"
	"api-caixa/utils"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Fechar fecha o caixa e gera um relatorio de pagamentos do dia e atualiza o caixaseq
func Fechar(dynamoClient *dynamodb.Client, log logar.Logfile) {
	pagamentoReport := make([]model.PagamentoReport, 0)
	caixa := query.GetLatestCaixa(dynamoClient, log)
	pagamentos := query.GetPagamentosAfterDate(dynamoClient, log, caixa.Dia)
	caixaNovo := model.Caixa{
		Dia:              time.Now().Format("2006-01-02_15:04:05"),
		DinheiroAbertura: caixa.DinheiroFechamento,
	}
	var TotalDebito float64
	var TotalPersyCoins float64
	var TotalPicPay float64
	var TotalPix float64
	var TotalCredito float64
	TotalDinheiro := caixa.DinheiroAbertura
	for _, pagamento := range pagamentos {
		formaPagamento := ""
		valor := 0.0
		if pagamento.Debito > 0 {
			TotalDebito += pagamento.Debito
			valor += pagamento.Debito
			formaPagamento += "Debito : " + utils.FormatCurrency(pagamento.Debito)
		} else if pagamento.PersyCoins > 0 {
			TotalPersyCoins += pagamento.PersyCoins
			valor += pagamento.PersyCoins
			formaPagamento = "PersyCoins " + utils.FormatCurrency(pagamento.PersyCoins)
		} else if pagamento.PicPay > 0 {
			TotalPicPay += pagamento.PicPay
			valor += pagamento.PicPay
			formaPagamento = "PicPay " + utils.FormatCurrency(pagamento.PicPay)
		} else if pagamento.Pix > 0 {
			TotalPix += pagamento.Pix
			valor += pagamento.Pix
			formaPagamento = "Pix " + utils.FormatCurrency(pagamento.Pix)
		} else if pagamento.Credito > 0 {
			TotalCredito += pagamento.Credito
			valor += pagamento.Credito
			formaPagamento = "Credito " + utils.FormatCurrency(pagamento.Credito)
		} else if pagamento.Dinheiro > 0 {
			TotalDinheiro += pagamento.Dinheiro
			valor += pagamento.Dinheiro
			formaPagamento = "Dinheiro " + utils.FormatCurrency(pagamento.Dinheiro)
		}
		pagamentoReport = append(pagamentoReport, model.PagamentoReport{
			Cliente:         pagamento.Cliente,
			FormasPagamento: formaPagamento,
			Valor:           valor,
			Data:            pagamento.Data,
		})
	}
	caixaNovo.TotalDebito = TotalDebito
	caixaNovo.TotalPersyCoins = TotalPersyCoins
	caixaNovo.TotalPicPay = TotalPicPay
	caixaNovo.TotalPix = TotalPix
	caixaNovo.TotalCredito = TotalCredito
	caixaNovo.DinheiroFechamento = TotalDinheiro
	caixaNovo.PagamentoReport = pagamentoReport
	query.InsertCaixa(dynamoClient, log, caixaNovo)
}
