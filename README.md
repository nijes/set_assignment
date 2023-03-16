# Set Assignment
> 2 이상의 파일(xlsx, csv, tsv, jsonl 등)에서 지정한 key column을 기준으로 set operations(diff, intersection, union)을 하는 프로그램 작성

---

## 2023.03.13
* 과제 내용 확인
* 실행시 프로그램에 필요한 인자값 받는 부분 작성
    * argparse 모듈 사용

---

## 2023.03.14
* 구현 계획
   1. 2개의 엑셀파일에 대해 1개의 key_column에 대한 연산
   2. 2이상의 key_column에 대한 처리(all, any)
   3. 3개 이상의 입력파일에 대한 처리
   4. excel 외의 파일 포맷에 대한 처리
   5. 기존 pandas로 구현한 기능 dask, ploars로 대체
* 2개의 데이터프레임에 대해 각 intersection, diff, union 연산 수행 구현
    <!-- * intersection(교집합)
        - 하나 이상의 key column 조건 만족
            ~~~python
            for key_column in key_columns:
                df1_set, df2_set = set(df1[key_column]), set(df2[key_column])
                if key_column == key_columns[0]:
                    inter_df = df1[df1[key_column].isin(df1_set&df2_set)]
                else:
                    inter_df = pd.concat([inter_df, df1[df1[key_column].isin(df1_set&df2_set)]], ignore_index=True).drop_duplicates()
            ~~~
        - 모든 key column 조건 만족
            ~~~python
            for key_column in key_columns:
                df1_set, df2_set = set(df1[key_column]), set(df2[key_column])
                df1 = df1[df1[key_column].isin(df1_set&df2_set)]
                df2 = df2[df2[key_column].isin(df1_set&df2_set)]
            inter_df = df1
            ~~~
    * diff(차집합)
        - 하나 이상의 key column 조건 만족
            ~~~python
            for key_column in key_columns:
                df1_set, df2_set = set(df1[key_column]), set(df2[key_column])
                df1 = df1[df1[key_column].isin(df1_set-df2_set)]
                df2 = df2[df2[key_column].isin(df2_set-df1_set)]
            diff_df = df1
            ~~~
        - 모든 key column 조건 만족
            ~~~python
            for key_column in key_columns:
                df1_set, df2_set = set(df1[key_column]), set(df2[key_column])
                if key_column == key_columns[0]:
                    diff_df = df1[df1[key_column].isin(df1_set-df2_set)]
                else:
                    diff_df = pd.concat([diff_df, df1[df1[key_column].isin(df1_set-df2_set)]]).drop_duplicates()
            ~~~ -->
    * union(합집합)
        - 하나 이상의 key column 조건 만족
            ~~~python
            union_df = pd.concat([df1, df2]).drop_duplicates(subset=key_columns)
            ~~~
        - 모든 key column 조건 만족
            ~~~python
            union_df = pd.concat([df1, df2])
            for key_column in key_columns:
                union_df = union_df.drop_duplicates(subset=[key_column])            
            ~~~
* 고민사항 및 내일 할 일
    * operation은 한 가지만 입력 가능하도록 예외처리 필요
    * main함수에 있는 코드 추가 검증 후 모듈화
        * 변수, 함수명 등에 대한 작명법 통일 필요 -> 해당 부분 추가 정리할 것
    * 3이상의 input에 대한 처리

---

## 2023.03.15
* 기본 구상
    * setoperator.py 
        - PandasOperator, DaskOperator, PolarsOperator 각각 클래스 구현
        - 각 클래스에서 __call__ 함수를 통해 각 opertion(intersection, diff, union) 결과 dataframe 반환
        - 함수 인자\) DataFrame(iterable), keycolumn(iterable), for_any_key_columns(bool)
        - 함수 결과\) DataFrame
* dataframe 연산 방식 고민
    1. outer merge -> merged dataframe을 각 조건(both, left_only, right_only)에 따라 필터링 -> 필요한 column만 추출
    2. key_column 값을 set로 만들어 조건 생성 -> 원본 dataframe을 각 조건에 따라 필터링(column 값이 해당 조건에 포함되는지) -> key column에 대해 반복문 수행
* 파일 입력 / 파일 저장 별도 구현
    * csv, xlsx
    * tsv 
        - csv 파일과 비슷하지만 ',' 대신 Tab으로 컬럼을 분리하는 파일포맷
    * jsonl 
        - 각 라인이 json객체로 이루어진 구조화된 데이터 형식
        - UTF-8 인코딩 + 각 행이 json 형식 + 각 행이 \n으로 구분
        - 참고 https://jsonlines.org/
        - jsonl to list
            ~~~python
            import json
            def load_jsonl(input_path) -> list:
                data = []
                with open(input_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        data.append(json.loads(line.rstrip('\n|\r')))
                return data
            ~~~
* 내일 할 일
    * getArguments, readFiles, saveFiles 정리 후 이동 -> 외부에서 import하는 형태로
        * xlsx 외의 형식에 대해 sample 만들어서 잘 작동하는지 확인
        * pandas 외의 dataframe framework에서는 방식 다른 점 고려
        * 추후 파일 형식 추가 용이하도록
    * dataframe 연산 방식에 대해 좀 더 고민해보기

---

## 2023.03.16
* A파일과 B파일의 column이 다른 경우 처리
    * 결과 파일의 column은 A파일과 같게
    * union 연산에서 B파일에 A파일의 일부 column이 없을 때 -> 해당 column의 값 비워두기
    * **key column이 존재하지 않는 경우** 어떻게 처리할 지 문제
* (기존 방식)모든 파일을 데이터프레임 형태로 읽어온 후 연산 
    * 연산 시 차례대로 하나씩 불러오는 방식으로 변경
    * 실행 스크립트가 아닌 모듈 내부에서 readdf함수 호출
* union 연산
    * 기준 dataframe에서 key_column값이 중복되는 경우가 있을 수 있어, drop_duplicates 사용 어려움
    * for_any_key_columns O : key_column 값이 하나라도 일치하면 결과값에서 제거
    * for_any_key_columns X : key_column 값이 전부 일치하면 결과값에서 제거
* xlsx 외의 포맷에 대해 테스트 (정상 동작 확인)
    * sample file 만들기
        - csv
            ~~~python
            import csv
            def save_csv(file_name, data):
                with open(file_name, 'w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerows(data)
            ~~~
        - tsv
            - csv에서 writer의 인자로 delemiter='\t'만 추가
        - jsonl
            ~~~~python
            import json
            def save_jsonl(file_name, data):
            with open(file_name, 'w', encoding='utf-8') as file:
                for json_obj in data:
                    file.write(json.dumps(json_obj) + '\n')
            ~~~~
* getargs, readdf, savedf 함수 위치 조정
* 내일 할 일
    * 지금까지의 진행 내용 정리 후 리뷰
    * key_column이 존재하지 않는 파일 입력받은 경우 처리 방법 고민
    * DASK, polars 공부