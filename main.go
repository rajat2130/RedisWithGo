package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
)

type Database struct {
	Client *redis.Client
}

var (
	ErrNil = errors.New("No mataching record found in redis database")
	Ctx    = context.TODO()
)

// Database Part---------------------------------------------------------------
func NewDatabase(address string) (*Database, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: "",
		DB:       0,
	})

	if err := client.Ping(Ctx).Err(); err != nil {
		return nil, err

	}
	return &Database{
		Client: client,
	}, nil
}

// Main Func Here you can call all the methods
var (
	ListenAddr = "localhost:8080"
	RedisAddr  = "localhost:6379"
)

func main() {
	database, err := NewDatabase(RedisAddr)
	if err != nil {
		log.Fatalf("Failed to connect redis database: %s", err.Error())
		log.Println(err)

	}
	router := initRouter(database)

	router.Run(ListenAddr)

}

func initRouter(database *Database) *gin.Engine {
	r := gin.Default()
	r.POST("/points", func(c *gin.Context) {
		var userJson User
		if err := c.ShouldBindJSON(&userJson); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		err := database.SaveUser(&userJson)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"user": userJson})
	})
	r.GET("/points/:username", func(c *gin.Context) {
		username := c.Param("username")
		user, err := database.GetUser(username)
		if err != nil {
			if err == ErrNil {
				c.JSON(http.StatusNotFound, gin.H{"error": "No record found for " + username})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"user": user})
	})
	r.GET("/leaderboard", func(c *gin.Context) {
		leaderboard, err := database.GetLeaderboard()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"leaderboard": leaderboard})
	})

	return r
}

// Leaderboard

var leaderboardKey = "leaderboard"

type Leaderboard struct {
	Count int `json:"count"`
	Users []*User
}

func (db *Database) GetLeaderboard() (*Leaderboard, error) {
	scores := db.Client.ZRangeWithScores(Ctx, leaderboardKey, 0, -1)
	if scores == nil {
		return nil, ErrNil
	}
	count := len(scores.Val())
	users := make([]*User, count)
	for idx, member := range scores.Val() {
		users[idx] = &User{
			Username: member.Member.(string),
			Points:   int(member.Score),
			Rank:     idx,
		}
	}
	leaderboard := &Leaderboard{
		Count: count,
		Users: users,
	}
	return leaderboard, nil
}

// User part
type User struct {
	Username string `json:"username" binding:"required"`
	Points   int    `json:"points" binding:"required"`
	Rank     int    `json:"rank"`
}

func (db *Database) SaveUser(user *User) error {
	member := &redis.Z{
		Score:  float64(user.Points),
		Member: user.Username,
	}
	pipe := db.Client.TxPipeline()
	pipe.ZAdd(Ctx, "leaderboard", *member)
	rank := pipe.ZRank(Ctx, leaderboardKey, user.Username)
	_, err := pipe.Exec(Ctx)
	if err != nil {
		return err
	}
	fmt.Println(rank.Val(), err)
	user.Rank = int(rank.Val())
	return nil
}

func (db *Database) GetUser(username string) (*User, error) {
	pipe := db.Client.TxPipeline()
	score := pipe.ZScore(Ctx, leaderboardKey, username)
	rank := pipe.ZRank(Ctx, leaderboardKey, username)
	_, err := pipe.Exec(Ctx)
	if err != nil {
		return nil, err
	}
	if score == nil {
		return nil, ErrNil
	}
	return &User{
		Username: username,
		Points:   int(score.Val()),
		Rank:     int(rank.Val()),
	}, nil
}
