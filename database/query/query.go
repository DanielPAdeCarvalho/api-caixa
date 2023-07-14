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
	seq--
	seqStr := fmt.Sprint(seq)
	params := &dynamodb.QueryInput{
		TableName:                 aws.String("Caixa"),
		KeyConditionExpression:    aws.String("Seq = :seqValue"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":seqValue": &types.AttributeValueMemberN{Value: seqStr}},
	}
	resp, err := dynamoClient.Query(context.TODO(), params)
	if err != nil {
		fmt.Println("Got error calling Query de GetLatestMoney", err)
		return 0.0
	}

	// Check if resp.Items is not empty
	if len(resp.Items) > 0 {
		caixa := model.Caixa{}
		err = attributevalue.UnmarshalMap(resp.Items[0], &caixa)
		if err != nil {
			fmt.Printf("Failed to unmarshal Record, %v para o caixa em GetLatestMoney", err)
			return 0.0
		}
		return caixa.DinheiroFechamento
	}

	fmt.Println("No items returned from Query de GetLatestMoney with seq = ", seq)
	return 0.0
}

// GetCaixaByDate retorna o caixa de uma data específica
func GetPagamentos(dynamoClient *dynamodb.Client, log logar.Logfile, seq int) []model.Pagamento {
	seqStr := fmt.Sprint(seq)
	params := &dynamodb.QueryInput{
		TableName:                 aws.String("Pagamentos"),
		IndexName:                 aws.String("Seq-index"), // specify the index SEQ-index
		KeyConditionExpression:    aws.String("Seq = :seqValue"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":seqValue": &types.AttributeValueMemberN{Value: seqStr}},
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

// Retorna o sequencial do CaixaSeq
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

// Atualiza o squencial para o proximo numero
func UpdateSeq(client *dynamodb.Client, log logar.Logfile, seq int) {
	antigo := fmt.Sprint(seq)
	novo := fmt.Sprint(seq + 1)

	// Delete the old item
	deleteParams := &dynamodb.DeleteItemInput{
		TableName: aws.String("CaixaSeq"),
		Key: map[string]types.AttributeValue{
			"Seq": &types.AttributeValueMemberN{Value: antigo},
		},
	}

	_, err := client.DeleteItem(context.TODO(), deleteParams)
	if err != nil {
		fmt.Println("Got error calling DeleteItem: ", err)
		return
	}

	// Insert the new item
	putParams := &dynamodb.PutItemInput{
		TableName: aws.String("CaixaSeq"),
		Item: map[string]types.AttributeValue{
			"Seq": &types.AttributeValueMemberN{Value: novo},
		},
	}

	_, err = client.PutItem(context.TODO(), putParams)
	if err != nil {
		fmt.Println("Got error calling PutItem: ", err)
		return
	}
}

// Retorna o ultimo caixa cadastrado em Caixa
func GetLatestCaixa(client *dynamodb.Client, log logar.Logfile) model.Caixa {
	seq := ReturnSeq(client, log)
	seq--
	fmt.Println("seq = ", seq)
	seqSrt := fmt.Sprint(seq)
	params := &dynamodb.QueryInput{
		TableName:                 aws.String("Caixa"),
		KeyConditionExpression:    aws.String("Seq = :seqVal"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":seqVal": &types.AttributeValueMemberN{Value: seqSrt}},
	}

	resp, err := client.Query(context.TODO(), params)
	if err != nil {
		fmt.Println("Got error calling Query: ", err)
		return model.Caixa{}
	}

	caixa := model.Caixa{}
	for _, item := range resp.Items {
		err = attributevalue.UnmarshalMap(item, &caixa)
		if err != nil {
			fmt.Printf("Failed to unmarshal Record, %v", err)
		}
	}
	return caixa
}
