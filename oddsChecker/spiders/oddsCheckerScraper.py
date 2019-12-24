# -*- coding: utf-8 -*-
import scrapy
import bs4 as bs
import csv
from urllib.parse import urljoin
import os


class OddscheckerscraperSpider(scrapy.Spider):
    name = 'oddsCheckerScraper'
    allowed_domains = ['www.oddschecker.com']
    start_urls = ['http://www.oddschecker.com']

    #start parsing
    def parse(self, response):
        sports = []

        sports.append('/basketball')
        #sports.append('/american-football')
        #sports.append('/football')
        #sports.append('/darts/pdc-world-championship')

        for sport in sports:
            url = urljoin(response.url, sport)
            yield scrapy.Request(url, callback=self.parse_sport,
                                      meta={'sport': sport},
                                      dont_filter=True)

    #find available games
    def parse_sport(self, response):
        sport = response.meta.get('sport')

        games_match_on = response.xpath('//*[@class="match-on "]')
        games_no_border = response.xpath('//*[@class="match-on no-top-border "]')

        #for every game today, find link, home team, away team
        for game in games_match_on + games_no_border:
            soup = bs.BeautifulSoup(game.extract(), features="html.parser")
            anchor = soup.findAll("a", {"class": "beta-callout"})
            if anchor:
                link = anchor[0]['href']
                event_name = anchor[0]['data-event-name'].strip()
                if len(event_name.split("at")) == 2:
                    home_team = event_name.split("at")[1].strip()
                    away_team = event_name.split("at")[0].strip()
                elif len(event_name.split("v")) == 2:
                    home_team = event_name.split("v")[0].strip()
                    away_team = event_name.split("v")[1].strip()
                else:
                    home_team = 'ERROR'
                    away_team = 'ERROR'

                # follow each link to scrap odds
                url = urljoin(response.url, link)
                yield scrapy.Request(url, callback=self.parse_game_winner,
                                     meta={'event_name': event_name,
                                           'home_team': home_team,
                                           'away_team': away_team,
                                           'sport': sport},
                                     dont_filter=True)

    # scrap odds for single winner
    def parse_game_winner(self, response):
        dict = {}

        rows = response.xpath('//tr[@class="diff-row evTabRow bc"]')
        for team in rows:
            data = team.extract()
            soup = bs.BeautifulSoup(data, features="html.parser")
            odds = soup.select('td[class*="bc bs"]')
            for odd in odds:
                name_of_bookie = odd.get("data-bk").strip()
                coeef = odd.get("data-odig").strip()

                if name_of_bookie not in dict:
                    dict[name_of_bookie] = [coeef]
                else:
                    dict[name_of_bookie].append(coeef)

        #create necessary folders
        directory = "./game_winners/" +  response.meta.get('sport').split('/')[1] + "/"
        try:
            os.mkdir("./game_winners")
        except OSError as e:
            print("Directory exists")
        try:
            os.mkdir(directory)
        except OSError as e:
            print("Directory exists")

        #write data to csv file
        csv_name = directory + response.meta.get('event_name') + '.csv'
        print(csv_name)
        with open(csv_name, mode='w') as game_one:
            values = list(dict.values())[0]
            games_with_draw = len(values) == 3 and response.meta.get('sport') == '/football'
            games_no_draw = len(values) == 2 and response.meta.get('sport') != '/football'

            if games_with_draw:
                fieldnames = ['bookie', response.meta.get('home_team'), 'Draw', response.meta.get('away_team')]
                game_writer = csv.DictWriter(game_one, fieldnames=fieldnames)
                game_writer.writeheader()
            elif games_no_draw:
                fieldnames = ['bookie', response.meta.get('away_team'), response.meta.get('home_team')]
                game_writer = csv.DictWriter(game_one, fieldnames=fieldnames)
                game_writer.writeheader()

            for bookie in dict:
                if games_no_draw:
                    curr_odd = {'bookie': bookie,
                                response.meta.get('away_team'): dict[bookie][0],
                                response.meta.get('home_team'): dict[bookie][1]}
                    game_writer.writerow(curr_odd)
                
                elif games_with_draw:
                    curr_odd = {'bookie': bookie, 
                                 response.meta.get('home_team'): dict[bookie][0],
                                 'Draw': dict[bookie][1],
                                 response.meta.get('away_team'): dict[bookie][2]}
                    game_writer.writerow(curr_odd)
