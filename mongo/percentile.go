package mongo

import (
	"go.mongodb.org/mongo-driver/bson"
)

func percentileCalc(percent float64) bson.M {
	p := percent / 100

	return bson.M{
		"$add": []any{
			bson.M{
				"$arrayElemAt": []any{
					"$values",
					bson.M{
						"$floor": bson.M{
							"$multiply": []any{
								p,
								bson.M{"$subtract": []any{bson.M{"$size": "$values"}, 1}},
							},
						},
					},
				},
			},
			bson.M{
				"$multiply": []any{
					bson.M{
						"$subtract": []any{
							bson.M{
								"$arrayElemAt": []any{
									"$values",
									bson.M{
										"$ceil": bson.M{
											"$multiply": []any{
												p,
												bson.M{"$subtract": []any{bson.M{"$size": "$values"}, 1}},
											},
										},
									},
								},
							},
							bson.M{
								"$arrayElemAt": []any{
									"$values",
									bson.M{
										"$floor": bson.M{
											"$multiply": []any{
												p,
												bson.M{"$subtract": []any{bson.M{"$size": "$values"}, 1}},
											},
										},
									},
								},
							},
						},
					},

					bson.M{
						"$mod": []any{
							bson.M{
								"$multiply": []any{
									p,
									bson.M{"$subtract": []any{bson.M{"$size": "$values"}, 1}},
								},
							},
							1,
						},
					},
				},
			},
		},
	}
}

// {
// 	"$project": bson.M{
// 		"values": "$values",
// 		"val": bson.M{
// 			"$add": []any{
// 				bson.M{
// 					"$arrayElemAt": []any{
// 						"$values",
// 						bson.M{
// 							"$floor": bson.M{
// 								"$multiply": []any{
// 									0.95,
// 									bson.M{"$subtract": []any{bson.M{"$size": "$values"}, 1}},
// 								},
// 							},
// 						},
// 					},
// 				},
// 				bson.M{
// 					"$multiply": []any{
// 						bson.M{
// 							"$subtract": []any{
// 								bson.M{
// 									"$arrayElemAt": []any{
// 										"$values",
// 										bson.M{
// 											"$ceil": bson.M{
// 												"$multiply": []any{
// 													0.95,
// 													bson.M{"$subtract": []any{bson.M{"$size": "$values"}, 1}},
// 												},
// 											},
// 										},
// 									},
// 								},
// 								bson.M{
// 									"$arrayElemAt": []any{
// 										"$values",
// 										bson.M{
// 											"$floor": bson.M{
// 												"$multiply": []any{
// 													0.95,
// 													bson.M{"$subtract": []any{bson.M{"$size": "$values"}, 1}},
// 												},
// 											},
// 										},
// 									},
// 								},
// 							},
// 						},

// 						bson.M{
// 							"$mod": []any{
// 								bson.M{
// 									"$multiply": []any{
// 										0.95,
// 										bson.M{"$subtract": []any{bson.M{"$size": "$values"}, 1}},
// 									},
// 								},
// 								1,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	},
// },
