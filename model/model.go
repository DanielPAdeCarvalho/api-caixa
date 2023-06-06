package model

type Caixa struct {
	Seq                int               `dynamodbav:"Seq,N"`
	Dia                string            `dynamodbav:"Dia,S"`
	DinheiroAbertura   float64           `dynamodbav:"DinheiroAbertura,N"`
	DinheiroFechamento float64           `dynamodbav:"DinheiroFechamento,N"`
	TotalDebito        float64           `dynamodbav:"TotalDebito,N"`
	TotalCredito       float64           `dynamodbav:"TotalCredito,N"`
	TotalPersyCoins    float64           `dynamodbav:"TotalPersyCoins,N"`
	TotalPicPay        float64           `dynamodbav:"TotalPicPay,N"`
	TotalPix           float64           `dynamodbav:"TotalPix,N"`
	DummyKey           string            `dynamodbav:"DummyKey,S"`
	PagamentoReport    []PagamentoReport `dynamodbav:"PagamentoReport,L"`
}

type Pagamento struct {
	Seq        int     `json:"Seq"`
	Cliente    string  `json:"Cliente"`
	Troco      float64 `json:"Troco"`
	Credito    float64 `json:"Credito"`
	Debito     float64 `json:"Debito"`
	Dinheiro   float64 `json:"Dinheiro"`
	PicPay     float64 `json:"Picpay"`
	Pix        float64 `json:"Pix"`
	PersyCoins float64 `json:"Persycoins"`
	Data       string  `json:"Data"`
}

type PagamentoReport struct {
	Cliente         string  `json:"Cliente"`
	FormasPagamento string  `json:"FormasPagamento"`
	Valor           float64 `json:"Valor"`
	Data            string  `json:"Data"`
}
