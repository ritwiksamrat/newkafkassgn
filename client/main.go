package main

import (
	// "strconv"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ritwiksamrat/newkafkassgn/proto"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:4040", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := proto.NewKafkaserviceClient(conn)

	g := gin.Default()
	g.GET("/apicall/:a/:b", func(ctx *gin.Context) {
		a := ctx.Param("a")
		b := ctx.Param("b")
		req := &proto.Request{
			Sub: string(a),
			Val: string(b),
		}
		if response, err := client.Apiservice(ctx, req); err == nil {
			ctx.JSON(http.StatusOK, gin.H{
				"result": fmt.Sprint(response.Result),
			})
		} else {
			ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	if err := g.Run(":9090"); err != nil {
		log.Fatalf("Failed	to run server: %v", err)
	}
}
