package caixa

import (
	"api-caixa/database/query"
	"api-caixa/logar"
	"api-caixa/model"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Fechar fecha o caixa e gera um relatorio de pagamentos do dia e atualiza o caixaseq
func Fechar(dynamoClient *dynamodb.Client, log logar.Logfile) {
	caixa := model.Caixa{}

	seq := query.ReturnSeq(dynamoClient, log)
	caixa.Seq = seq + 1 // Incrementa o caixaSeq

	t := time.Now()
	dia := t.Format("2006-01-02_15:04:05")
	caixa.Dia = dia

	dinheiroAbertura := query.GetLatestMoney(dynamoClient, log, seq)
	caixa.DinheiroAbertura = dinheiroAbertura

	pagamentos := query.GetPagamentos(dynamoClient, log, caixa.Seq)
	DinheiroFechamento := dinheiroAbertura

	// Cria o array de formas de pagamento para o pagamentoReport
	pagamentoReportS := make([]model.PagamentoReport, 0)
	for _, p := range pagamentos {
		pagamentoReport := model.PagamentoReport{
			Cliente: p.Cliente,
			Dia:     p.Data,
		}
		Valor := 0.0
		formasPagamento := make([]string, 0)
		if p.Credito > 0 {
			Valor += p.Credito
			creditStr := fmt.Sprintf("%.2f", p.Credito) // convert float64 to string with 2 decimal points
			result := "Credito: " + creditStr
			formasPagamento = append(formasPagamento, result)
			caixa.TotalCredito += p.Credito
		}
		if p.Debito > 0 {
			Valor += p.Debito
			debitStr := fmt.Sprintf("%.2f", p.Debito) // convert float64 to string with 2 decimal points
			result := "Debito: " + debitStr
			formasPagamento = append(formasPagamento, result)
			caixa.TotalDebito += p.Debito
		}
		if p.Dinheiro > 0 {
			Valor += p.Dinheiro
			dinheiroStr := fmt.Sprintf("%.2f", p.Dinheiro) // convert float64 to string with 2 decimal points
			result := "Dinheiro: " + dinheiroStr
			formasPagamento = append(formasPagamento, result)
			DinheiroFechamento += p.Dinheiro
		}
		if p.PicPay > 0 {
			Valor += p.PicPay
			picpayStr := fmt.Sprintf("%.2f", p.PicPay) // convert float64 to string with 2 decimal points
			result := "PicPay: " + picpayStr
			formasPagamento = append(formasPagamento, result)
			caixa.TotalPicPay += p.PicPay
		}
		if p.Pix > 0 {
			Valor += p.Pix
			pixStr := fmt.Sprintf("%.2f", p.Pix) // convert float64 to string with 2 decimal points
			result := "Pix: " + pixStr
			formasPagamento = append(formasPagamento, result)
			caixa.TotalPix += p.Pix
		}
		if p.PersyCoins > 0 {
			Valor += p.PersyCoins
			persycoinsStr := fmt.Sprintf("%.2f", p.PersyCoins) // convert float64 to string with 2 decimal points
			result := "PersyCoins: " + persycoinsStr
			formasPagamento = append(formasPagamento, result)
			caixa.TotalPersyCoins += p.PersyCoins
		}
		if p.Troco > 0 {
			DinheiroFechamento -= p.Troco
		}
		pagamentoReport.Valor = Valor
		pagamentoReport.FormasPagamento = formasPagamento
		pagamentoReportS = append(pagamentoReportS, pagamentoReport)
	}
	caixa.DinheiroFechamento = DinheiroFechamento
	caixa.PagamentoReport = pagamentoReportS
	query.InsertCaixa(dynamoClient, log, caixa)
	query.UpdateSeq(dynamoClient, log, caixa.Seq)
}
