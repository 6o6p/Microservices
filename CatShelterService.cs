using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microservices.Common.Exceptions;
using Microservices.ExternalServices.Authorization;
using Microservices.ExternalServices.Authorization.Types;
using Microservices.ExternalServices.Billing;
using Microservices.ExternalServices.Billing.Types;
using Microservices.ExternalServices.CatDb;
using Microservices.ExternalServices.CatDb.Types;
using Microservices.ExternalServices.CatExchange;
using Microservices.ExternalServices.CatExchange.Types;
using Microservices.ExternalServices.Database;
using Microservices.Types;

namespace Microservices
{
    public class CatShelterService : ICatShelterService
    {
        private readonly IDatabase _db;
        private readonly IAuthorizationService _authorizationService;
        private readonly IBillingService _billingService;
        private readonly ICatInfoService _catInfoService;
        private readonly ICatExchangeService _catExchangeService;

        public CatShelterService(
            IDatabase database,
            IAuthorizationService authorizationService,
            IBillingService billingService,
            ICatInfoService catInfoService,
            ICatExchangeService catExchangeService)
        {
            _db = database;
            _authorizationService = authorizationService;
            _billingService = billingService;
            _catInfoService = catInfoService;
            _catExchangeService = catExchangeService;
        }

        public async Task<List<Cat>> GetCatsAsync(string sessionId, int skip, int limit, 
                                                  CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var billing = await TryTwice(() => _billingService.GetProductsAsync(skip, limit, cancellationToken));  

            return billing
                .Select(async product => await GetCatAsync(product.Id, cancellationToken))
                .Select(t => t.Result)
                .ToList();
        }

        public async Task AddCatToFavouritesAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var userFavorites = await FindInCollectionAsync<UserFavorites>("Favorites", user.UserId, cancellationToken);

            if (userFavorites == null)
            {
                userFavorites = new UserFavorites { Id = user.UserId, Favorites = new HashSet<Guid>() };
            }

            userFavorites.Favorites.Add(catId);

            await WriteToCollectionAsync("Favorites", userFavorites, cancellationToken);
        }

        public async Task<List<Cat>> GetFavouriteCatsAsync(string sessionId, CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var userFavorites = await FindInCollectionAsync<UserFavorites>("Favorites", user.UserId, cancellationToken);

            //var cats = new List<Cat>();

            //if (userFavorites != null && userFavorites.Favorites != null)
            //{
            //    foreach (var id in userFavorites.Favorites)
            //    {
            //        if (await GetProductAsync(id, cancellationToken) != null)
            //        {
            //            cats.Add(await GetCatAsync(id, cancellationToken));
            //        }
            //    }
            //}

            //return cats;

            return userFavorites == null || userFavorites.Favorites == null
                ? new List<Cat>()
                : userFavorites.Favorites
                    .Where(id => GetProductAsync(id, cancellationToken) != null)
                    .Select(async id => await GetCatAsync(id, cancellationToken))
                    .Select(t => t.Result)
                    .ToList();
        }

        public async Task DeleteCatFromFavouritesAsync(string sessionId, Guid catId, 
                                                       CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var userFavorites = await FindInCollectionAsync<UserFavorites>("Favorites", user.UserId, cancellationToken);

            if (userFavorites != null && userFavorites.Favorites != null)
            {
                userFavorites.Favorites.Remove(catId);
                await WriteToCollectionAsync("Favorites", userFavorites, cancellationToken);
            }
        }

        public async Task<Bill> BuyCatAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var product = await GetProductAsync(catId, cancellationToken);

            if (product == null)
                throw new InvalidRequestException();

            var cat = await GetCatAsync(product.Id, cancellationToken);

            return cat == null
                ? null    
                : await TryTwice(() => _billingService.SellProductAsync(catId, cat.Price, cancellationToken));
        }

        public async Task<Guid> AddCatAsync(string sessionId, AddCatRequest request, 
                                            CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var catInfo = await TryTwice(() => _catInfoService.FindByBreedNameAsync(request.Breed, cancellationToken));

            var priceInfo = await GetPriceHistoryAsync(catInfo.BreedId, cancellationToken);

            var latestPrice = GetCatPriceInfo(priceInfo);

            var catEntity = new CatEntity()
            {
                Id = Guid.NewGuid(),
                BreedId = catInfo.BreedId,
                AddedBy = user.UserId,
                Name = request.Name,
                CatPhoto = request.Photo
            };

            await TryTwice(() => _billingService.AddProductAsync(
                new Product { Id = catEntity.Id, BreedId = catEntity.BreedId }, 
                cancellationToken
            ));

            await WriteToCollectionAsync("CatEntities", catEntity, cancellationToken);

            return catEntity.Id;
        }

        private class CatEntity : IEntityWithId<Guid>
        {
            public Guid Id { get; set; }
            public Guid BreedId { get; set; }
            public Guid AddedBy { get; set; }
            public string Name { get; set; }
            public byte[] CatPhoto { get; set; }
        }

        private class UserFavorites : IEntityWithId<Guid>
        {
            public Guid Id { get; set; }
            public HashSet<Guid> Favorites { get; set; }
        }

        private async Task<T> TryTwice<T>(Func<Task<T>> func)
        {
            try
            {
                return await func();
            }
            catch (ConnectionException)
            {
                try
                {
                    return await func();
                }
                catch (ConnectionException)
                {
                    throw new InternalErrorException();
                }
            }
        }

        private async Task TryTwice(Func<Task> func)
        {
            try
            {
                await func();
            }
            catch (ConnectionException)
            {
                try
                {
                    await func();
                }
                catch (ConnectionException)
                {
                    throw new InternalErrorException();
                }
            }
        }

        private async Task<AuthorizationResult> TryAuthorizeAsync(string sessionId, CancellationToken cancellationToken)
        {
            var result = await TryTwice(() => _authorizationService.AuthorizeAsync(sessionId, cancellationToken));

            return result.IsSuccess
                ? result
                : throw new AuthorizationException();
        }

        private async Task<Product> GetProductAsync(Guid id, CancellationToken cancellationToken) =>
            await TryTwice(() => _billingService.GetProductAsync(id, cancellationToken));

        private async Task<CatPriceHistory> GetPriceHistoryAsync(Guid breedId, CancellationToken cancellationToken) =>
            await TryTwice(() => _catExchangeService.GetPriceInfoAsync(breedId, cancellationToken));

        private async Task<T> FindInCollectionAsync<T>(string collection, Guid id, 
                                                       CancellationToken cancellationToken) 
            where T : class, IEntityWithId<Guid> =>
                await TryTwice<T>(() => _db.GetCollection<T, Guid>(collection).FindAsync(id, cancellationToken));

        private async Task WriteToCollectionAsync<T>(string collection, T document,
                                                     CancellationToken cancellationToken)
            where T : class, IEntityWithId<Guid> =>
                await TryTwice(() => _db.GetCollection<T, Guid>(collection).WriteAsync(document, cancellationToken));

        private async Task<Cat> GetCatAsync(Guid catId, CancellationToken cancellationToken)
        {
            var catEntity = await FindInCollectionAsync<CatEntity>("CatEntities", catId, cancellationToken);

            if (catEntity == null)
                return null;

            var catInfo = await TryTwice(() => _catInfoService.FindByBreedIdAsync(catEntity.BreedId, cancellationToken));

            var priceInfo = await GetPriceHistoryAsync(catInfo.BreedId, cancellationToken);

            var latestPrice = GetCatPriceInfo(priceInfo);

            return new Cat
            {
                Id = catEntity.Id,
                BreedId = catEntity.BreedId,
                AddedBy = catEntity.AddedBy,
                Breed = catInfo.BreedName,
                Name = catEntity.Name,
                CatPhoto = catEntity.CatPhoto,
                BreedPhoto = catInfo.Photo,
                Price = latestPrice.Price,
                Prices = priceInfo.Prices.Select(p => (p.Date, p.Price)).ToList()
            };
        }

        private CatPriceInfo GetCatPriceInfo(CatPriceHistory history) => 
            history.Prices != null && history.Prices.Count > 0
                ? history.Prices.Last()
                : new CatPriceInfo { Date = DateTime.Now, Price = 1000 };
    }
}