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
    * intersection(교집합)
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
            ~~~
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
