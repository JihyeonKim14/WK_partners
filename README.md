# WK_partners
2023.02 ~ 2023.06 Data scientist

# Task 별 사용되는 모듈 설명

# 1. ETL

FTC_ariflow.py
- 공정거래위원회 정보공개서의 데이터들을 수집 - 처리 - 저장 하는 DAG!
- 3개의 Task로 이루어져 있으며 각 task는 각각의 목적에 맞는 모듈을 제작하여 dags - utils 경로에 저장 및 불러오는 형태로 실행됨
- 아래는 각 모듈 별 설명
- 
- Task 1 : crawling
    위 모듈은 공정위 정보공개서에 새롭게 등록되는 data들을 크롤링하여 DB에 저장하는 일을 한다.
    저장되는 DB 테이블 명은 “FTC_MAIN_INDEX_HIST_TB” 이다.
    
- Task 2: Main_index_etl.py
-     
    위 코드는 각 프랜차이즈별 세부 정보가 기재된 html 파일을 네이버 클라우드에 s3에 html 형태 그대로 저장하는 코드이다. 여기서 저장된 html 파일들을 다음 Task에서 파싱하여 ftc 스키마에 각 테이블에 저장되는 구조
    
- Task 3: Html_etl.py
    
    s3에 저장된 html파일들을 파싱하여 DB에 저장하는 모듈
    
- slack_alert.py
    
    이 일련의 Task들을 하나의 DAG으로서 관리되는데, 이 DAG의 실행결과가 성공인지 실패인지 slack을 통해 전달하고 있고, 이를 가능케하는 모듈이 위 파일이다.
    

# 2. long_list

long_list_alert.py
- 공정위 크롤링을 바탕으로 롱리스트를 추출하고 추출 결과를 DB에 저장하며 excel 형태로 slack에 전송하는 DAG!
- 2개의 Task로 이루어져 있으며 각 task는 각각의 목적에 맞는 모듈을 제작하여 dags - utils 경로에 저장 및 불러오는 형태로 실행됨
- 아래는 각 모듈 별 설명
- 
- Task 1: Hypothesis.py
    
    롱리스트 산출을 위한 전처리 + 가설들로 이루어져 있음 
    
- Task 2: slack_long_list_alert.py
    
    산출된 롱리스트 결과를 excel화 하여 slack으로 보내는 모듈. 엑셀 파일은 4개의 sheet로 구성됨
