from datetime import datetime

users = [
    {
        "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoibnZfQSIsImV4cGlyZXMiOjE2NjA1NDEyNDkuMDM1Mjg2fQ.EyJZbeyynl4bwPN0G7VYP6dQw6fmm3Gh92TxJkOiozA", 
        "user_id": "u1",
        "city": "TPHCM",
        "district": "Phu Nhuan",
        "time": datetime.now()
    },
    {
        "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoibnZfQiIsImV4cGlyZXMiOjE2NjA1NDEyNzcuMTE1NjQ3fQ.-X6XapXG75Z_tuiHAjhOY01IjkB57-qJzrc-ybWctQ8", 
        "user_id": "u2",
        "city": "TPHCM",
        "district": "Go Vap",
        "time": datetime.now()
    },
    {
        "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoibnZfQyIsImV4cGlyZXMiOjE2NjA1NDEzMDYuMzYyMDMzfQ.c3TvkOPZOiEullUUPkKXpa5ugFRK_oO3_-1XMVLMuxE", 
        "user_id": "u3",
        "city": "Thu Duc",
        "district": "Go Vap",
        "time": datetime.now()
    },
    {
        "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiVFBfQSIsImV4cGlyZXMiOjE2NjA1NDEzMzYuMDUzMTgxfQ.pj5tRqB0V2v_X550qlakYR9_tFLS8MudkzG0J3aOMqc", 
        "user_id": "u4",
        "city": "Da Nang",
        "district": "Q2",
        "time": datetime.now()
    },
    {
        "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiUEJfQiIsImV4cGlyZXMiOjE2NjA1NDEzNjEuOTIwNzkyfQ.RwUXVIu82yX5QM9w84tM9kqfuuQnjxE13GF3d8rbWl0", 
        "user_id": "u5",
        "city": "Ha Noi",
        "district": "Cau Giay",
        "time": datetime.now()
    },
    {
        "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiVEJfQyIsImV4cGlyZXMiOjE2NjA1NDEzOTkuNzA0Njc3OH0.mrzNGBOyOMwxWT9sLGiTCjENtiye6Ckiqsw09-eXBAI", 
        "user_id": "u6",
        "city": "Hai Phong",
        "district": "Cau Giay",
        "time": datetime.now()
    },
]

delivery_worker = [
    {
        'user_id': 'u1',
        'parts': [1],
        'positions': [1]
    },
    {
        'user_id': 'u2',
        'parts': [2],
        'positions': [2]
    },
    {
        'user_id': 'u3',
        'parts': [2,3],
        'positions': [4,5]
    },
    {
        'user_id': 'u1u2u3',
        'parts': [1,5],
        'positions': [2,4]
    },
]

dummy = {
    "token": "kdsahsadbds", 
    "user_id": "u7",
    "city": "Thu Duc",
    "district": "Go Vap",
    "time": datetime.now()
},