from cassandra.cluster import Cluster
from flask import Flask
from flask import request
from flask import Response
from flask import jsonify
from flask_cors import CORS, cross_origin
import re

cluster = Cluster()


app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'

#http://localhost:80/recommendations?cid=BBID_211417787
@app.route("/recommendations", methods=['GET', 'POST'])
def recommendations():
	session = cluster.connect('rfm')
	session.set_keyspace("rfm")
	id = str(request.args.get('cid'));

	p = session.execute("SELECT * FROM recommendations WHERE cid='"+id+"'")

	j = dict()

	if not p:
		j['res'] = "Customer not found"
		return jsonify(j)	

	j['res']=mapRecommendations(p)

	return jsonify(j)

#http://localhost:80/rfm?cid=BBID_211417787
@app.route("/rfm", methods=['GET', 'POST'])
def rfm():
	session = cluster.connect('rfm')
	session.set_keyspace("rfm")
	id = str(request.args.get('cid'));

	p = session.execute("SELECT * FROM clustering WHERE customer='"+id+"'")

	j = dict()

	if not p:
		j['res'] = "Customer not found"
		return jsonify(j)	

	j['res']=mapClustering(p)

	return jsonify(j)

def mapClustering(p):
	a = []
	j = dict()

	j['Customer']=p[0].customer
	j['R']=p[0].recency
	j['F']=p[0].frequency
	j['M_value']=p[0].monetary_value
	j['R_Quartile']=p[0].r_quartile
	j['F_Quartile']=p[0].f_quartile
	j['M_Quartile']=p[0].m_quartile
	j['RFM']=p[0].rfm

	return j

def mapRecommendations(p):
	a = []
	for obj in p:
		s = re.findall('\[(.*?)\]',obj.reco)
		if not s:
			j = {
			"personal": "none",
				"total":{
				"0": (obj.reco).split()[0]+'-'+desc((obj.reco).split()[0]),
				"1": (obj.reco).split()[1]+'-'+desc((obj.reco).split()[1]),
				"2": (obj.reco).split()[2]+'-'+desc((obj.reco).split()[2])
				}
			} 
		else:
			p = re.findall('\[(.*?)\]',obj.reco)[0].split(',')
			t = re.findall('\[(.*?)\]',obj.reco)[1].split(',')
			j = dict()
			j["personal"]={}
			j["total"]={}
			for idx,r in enumerate(p): 
				j["personal"][idx] = r.strip()+'-'+desc(r.strip())
			for idx,r in enumerate(t):
				j["total"][idx] = r.strip()+'-'+desc(r.strip())

	return j

def desc(code):
	session = cluster.connect('rfm')
	session.set_keyspace("rfm")

	p = session.execute("SELECT * FROM products WHERE id='"+code+"'")

	if not p:
		return "Product not found"

	return str(p[0].description)	



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)