package mongo

import (
	"go.mongodb.org/mongo-driver/bson"
)

func percentileCalc(percent float64) bson.M {
	p := percent / 100

	return bson.M{
		"$add": []interface{}{
			bson.M{
				"$arrayElemAt": []interface{}{
					"$values",
					bson.M{
						"$floor": bson.M{
							"$multiply": []interface{}{
								p,
								bson.M{"$subtract": []interface{}{bson.M{"$size": "$values"}, 1}},
							},
						},
					},
				},
			},
			bson.M{
				"$multiply": []interface{}{
					bson.M{
						"$subtract": []interface{}{
							bson.M{
								"$arrayElemAt": []interface{}{
									"$values",
									bson.M{
										"$ceil": bson.M{
											"$multiply": []interface{}{
												p,
												bson.M{"$subtract": []interface{}{bson.M{"$size": "$values"}, 1}},
											},
										},
									},
								},
							},
							bson.M{
								"$arrayElemAt": []interface{}{
									"$values",
									bson.M{
										"$floor": bson.M{
											"$multiply": []interface{}{
												p,
												bson.M{"$subtract": []interface{}{bson.M{"$size": "$values"}, 1}},
											},
										},
									},
								},
							},
						},
					},

					bson.M{
						"$mod": []interface{}{
							bson.M{
								"$multiply": []interface{}{
									p,
									bson.M{"$subtract": []interface{}{bson.M{"$size": "$values"}, 1}},
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
// 			"$add": []interface{}{
// 				bson.M{
// 					"$arrayElemAt": []interface{}{
// 						"$values",
// 						bson.M{
// 							"$floor": bson.M{
// 								"$multiply": []interface{}{
// 									0.95,
// 									bson.M{"$subtract": []interface{}{bson.M{"$size": "$values"}, 1}},
// 								},
// 							},
// 						},
// 					},
// 				},
// 				bson.M{
// 					"$multiply": []interface{}{
// 						bson.M{
// 							"$subtract": []interface{}{
// 								bson.M{
// 									"$arrayElemAt": []interface{}{
// 										"$values",
// 										bson.M{
// 											"$ceil": bson.M{
// 												"$multiply": []interface{}{
// 													0.95,
// 													bson.M{"$subtract": []interface{}{bson.M{"$size": "$values"}, 1}},
// 												},
// 											},
// 										},
// 									},
// 								},
// 								bson.M{
// 									"$arrayElemAt": []interface{}{
// 										"$values",
// 										bson.M{
// 											"$floor": bson.M{
// 												"$multiply": []interface{}{
// 													0.95,
// 													bson.M{"$subtract": []interface{}{bson.M{"$size": "$values"}, 1}},
// 												},
// 											},
// 										},
// 									},
// 								},
// 							},
// 						},

// 						bson.M{
// 							"$mod": []interface{}{
// 								bson.M{
// 									"$multiply": []interface{}{
// 										0.95,
// 										bson.M{"$subtract": []interface{}{bson.M{"$size": "$values"}, 1}},
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
