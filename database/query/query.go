package query

import (
	"api-caixa/logar"
	"api-caixa/model"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// GetCaixaByDate retorna o caixa de uma data específica
func GetLatestMoney(dynamoClient *dynamodb.Client, log logar.Logfile, seq int) float64 {
	params := &dynamodb.QueryInput{
		TableName:                 aws.String("Caixa"),
		KeyConditionExpression:    aws.String("Seq = :seq"),
		ExpressionAttributeValues: map[string]types.AttributeValue{"seq": &types.AttributeValueMemberN{Value: *aws.String(fmt.Sprint(seq))}},
	}
	resp, err := dynamoClient.Query(context.TODO(), params)
	if err != nil {
		fmt.Println("Got error calling Query de getlatestcaiixa", err)
	}
	caixa := model.Caixa{}
	err = attributevalue.UnmarshalMap(resp.Items[0], &caixa)
	if err != nil {
		fmt.Printf("Failed to unmarshal Record, %v para o caixa em getlatestcaixa", err)
	}
	return caixa.DinheiroFechamento
}

// GetCaixaByDate retorna o caixa de uma data específica
func GetPagamentos(dynamoClient *dynamodb.Client, log logar.Logfile, seq int) []model.Pagamento {
	params := &dynamodb.QueryInput{
		TableName:                 aws.String("Pagamentos"),
		KeyConditionExpression:    aws.String("Seq = :seq"),
		ExpressionAttributeValues: map[string]types.AttributeValue{"seq": &types.AttributeValueMemberN{Value: *aws.String(fmt.Sprint(seq))}},
	}

	resp, err := dynamoClient.Query(context.TODO(), params)
	if err != nil {
		fmt.Println("Got error calling Query de getpagamentos", err)
		return nil
	}

	pagamentos := make([]model.Pagamento, 0)
	for _, item := range resp.Items {
		p := model.Pagamento{}
		err = attributevalue.UnmarshalMap(item, &p)
		if err != nil {
			fmt.Printf("Failed to unmarshal Record, %v para o array de pagamentos", err)
		}
		pagamentos = append(pagamentos, p)
	}
	return pagamentos
}

// InsertCaixa insere um novo caixa no banco de dados
func InsertCaixa(dynamoClient *dynamodb.Client, log logar.Logfile, caixa model.Caixa) {
	av, err := attributevalue.MarshalMap(caixa)
	if err != nil {
		fmt.Println("Got error marshalling new caixa item:", err)
		return
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String("Caixa"),
	}

	_, err = dynamoClient.PutItem(context.Background(), input)
	if err != nil {
		fmt.Println("Got error calling PutItem para insertcaixa", err)
		return
	}
}

func ReturnSeq(client *dynamodb.Client, log logar.Logfile) int {
	proj := expression.NamesList(expression.Name("Seq"))
	expr, err := expression.NewBuilder().WithProjection(proj).Build()
	logar.Check(err, log)

	input := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String("CaixaSeq"),
	}

	page, err := client.Scan(context.Background(), input)
	logar.Check(err, log)

	if len(page.Items) == 0 {
		return 0
	}

	var seq model.CaixaSeq
	err = attributevalue.UnmarshalMap(page.Items[0], &seq)
	logar.Check(err, log)

	return seq.Seq
}

func UpdateSeq(client *dynamodb.Client, log logar.Logfile, seq int) {
	antigo := fmt.Sprint(seq - 1)
	novo := fmt.Sprint(seq)
	params := &dynamodb.UpdateItemInput{
		TableName: aws.String("CaixaSeq"),
		Key: map[string]types.AttributeValue{
			"Seq": &types.AttributeValueMemberN{Value: antigo},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":s": &types.AttributeValueMemberN{Value: novo},
		},
		UpdateExpression: aws.String("set Seq = :s"),
		ReturnValues:     types.ReturnValueUpdatedNew,
	}

	_, err := client.UpdateItem(context.TODO(), params)
	if err != nil {
		fmt.Println("Got error calling UpdateItem: ", err)
		return
	}
}

func GetLatestCaixa(client *dynamodb.Client, log logar.Logfile) model.Caixa {
	seq := ReturnSeq(client, log)
	seqSrt := fmt.Sprint(seq)
	params := &dynamodb.QueryInput{
		TableName:                 aws.String("caixa"),
		KeyConditionExpression:    aws.String("seq = :seq"),
		ExpressionAttributeValues: map[string]types.AttributeValue{"seq": &types.AttributeValueMemberN{Value: seqSrt}},
	}

	resp, err := client.Query(context.TODO(), params)
	if err != nil {
		fmt.Println("Got error calling Query: ", err)
		return model.Caixa{}
	}

	c := model.Caixa{}
	for _, item := range resp.Items {
		err = attributevalue.UnmarshalMap(item, &c)
		if err != nil {
			fmt.Printf("Failed to unmarshal Record, %v", err)
		}
	}
	return c
}
